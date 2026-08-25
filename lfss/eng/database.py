""" High-level database action classes and functions.  """

import uuid, io, zipfile, datetime
from typing import Optional, AsyncIterable
import asyncio
import aiosqlite, aiofiles
import mimetypes, mimesniff

from .typing_helpers import override
from .config import MAX_MEM_FILE_BYTES, LARGE_FILE_BYTES, CHUNK_SIZE
from .log import get_logger
from .connection_pool import transaction, unique_cursor, execute_sql, TransactionHookBase
from .database_conn import FileConn, UserConn, validate_url, remove_external_blob
from .datatype import FileRecord, UserRecord, FileReadPermission, AccessLevel
from .permission import check_path_permission, check_file_read_permission
from .utils import concurrent_wrap, decode_uri_components, g_debounce_tasks
from .error import *

class DeferredFileTrash(TransactionHookBase):
    def __init__(self):
        self._schedule_deletion = set()
    
    async def schedule(self, file_id: str):
        self._schedule_deletion.add(file_id)
    
    async def run_deletion(self):
        async def ensure_deletion(file_id: str):
            try:
                await remove_external_blob(file_id)
            except Exception as e:
                get_logger('database', global_instance=True).error(f"Error deleting blob {file_id}: {e}")
        await asyncio.gather(*[ensure_deletion(f_id) for f_id in self._schedule_deletion])

    @override
    async def on_rollback(self):
        self._schedule_deletion.clear()

    @override
    async def on_commit(self):  # defer deletion to not block the transaction
        # register the task so that the shutdown path (wait_for_debounce_tasks) awaits it
        g_debounce_tasks.push(asyncio.create_task(self.run_deletion()))


async def get_user(cur: aiosqlite.Cursor, user: int | str) -> Optional[UserRecord]:
    uconn = UserConn(cur)
    if isinstance(user, str):
        return await uconn.get_user(user)
    elif isinstance(user, int):
        return await uconn.get_user_by_id(user)
    else:
        return None

# higher level database operations, mostly transactional
class Database:
    logger = get_logger('database', global_instance=True)

    async def init(self):
        async with transaction() as conn:
            await execute_sql(conn, 'init.sql')
        return self
    
    async def update_file_record(self, url: str, permission: FileReadPermission, op_user: Optional[UserRecord] = None):
        validate_url(url)
        async with transaction() as conn:
            fconn = FileConn(conn)
            r = await fconn.get_file_record(url)
            if r is None:
                raise PathNotFoundError(f"File {url} not found")
            if op_user is not None:
                if await check_path_permission(url, op_user) < AccessLevel.WRITE:
                    raise PermissionDeniedError(f"Permission denied: {op_user.username} cannot update file {url}")
            await fconn.update_file_record(url, permission=permission)
    
    async def save_file(
        self, u: int | str, url: str, 
        blob_stream: AsyncIterable[bytes], 
        permission: FileReadPermission = FileReadPermission.UNSET, 
        mime_type: Optional[str] = None
        ) -> int:
        """
        Save a file to the database. 
        Will check file size and user storage limit, 
        should check permission before calling this method. 
        The upload is rejected if size exceeds the user's storage limit.
        """
        validate_url(url)
        async with unique_cursor() as cur:
            user = await get_user(cur, u)
            assert user is not None, f"User {u} not found"

            if await check_path_permission(url, user, cursor=cur) < AccessLevel.WRITE:
                raise PermissionDeniedError(f"Permission denied: {user.username} cannot write to {url}")
            
            # snapshot of used size (on a read cursor), used for the early rejection cap below;
            # the authoritative check is re-done inside the write transaction.
            fconn_r = FileConn(cur)
            user_size_used = await fconn_r.user_size(user.id)

            f_id = uuid.uuid4().hex

        # reject early if the upload would exceed the quota, so we don't
        # buffer the entire body before failing
        upload_cap = max(user.max_storage - user_size_used, 0)

        file_size = 0
        async with aiofiles.tempfile.SpooledTemporaryFile(max_size=MAX_MEM_FILE_BYTES) as f:
            async for chunk in blob_stream:
                file_size += len(chunk)
                if file_size > upload_cap:
                    raise StorageExceededError(f"Unable to save file, user {user.username} has storage limit of {user.max_storage}, used at least {user_size_used}, upload rejected at {file_size} bytes")
                await f.write(chunk)
            
            # check mime type
            if mime_type is None:
                def custom_mime_checker(url: str) -> Optional[str]:
                    last_component = url.split('/')[-1]
                    if not '.' in last_component:
                        return None
                    ext = last_component.split('.')[-1].lower()
                    if ext == 'md':
                        return 'text/markdown'
                    return None
                mime_type = custom_mime_checker(url)
            if mime_type is None:
                mime_type, _ = mimetypes.guess_type(url)
            if mime_type is None:
                await f.seek(0)
                mime_type = mimesniff.what(await f.read(1024))
            if mime_type is None:
                mime_type = 'application/octet-stream'
            await f.seek(0)
            
            if file_size < LARGE_FILE_BYTES:
                blob = await f.read()
                async with transaction() as w_cur:
                    fconn_w = FileConn(w_cur)
                    # re-check the quota inside the write transaction (single writer => authoritative)
                    used_now = await fconn_w.user_size(user.id)
                    if used_now + file_size > user.max_storage:
                        raise StorageExceededError(f"Unable to save file, user {user.username} has storage limit of {user.max_storage}, used {used_now}, requested {file_size}")
                    await fconn_w.set_file_blob(f_id, blob)
                    await fconn_w.set_file_record(
                        url, owner_id=user.id, file_id=f_id, file_size=file_size, 
                        permission=permission, external=False, mime_type=mime_type)
            
            else:
                async def blob_stream_tempfile():
                    nonlocal f
                    while True:
                        chunk = await f.read(CHUNK_SIZE)
                        if not chunk: break
                        yield chunk
                # write the blob first, if the quota check below fails and the transaction
                # is rolled back, an orphan blob may remain on disk (lfss-vacuum will clean it)
                await FileConn.set_file_blob_external(f_id, blob_stream_tempfile())
                async with transaction() as w_cur:
                    fconn_w = FileConn(w_cur)
                    # re-check the quota inside the write transaction (single writer => authoritative)
                    used_now = await fconn_w.user_size(user.id)
                    if used_now + file_size > user.max_storage:
                        raise StorageExceededError(f"Unable to save file, user {user.username} has storage limit of {user.max_storage}, used {used_now}, requested {file_size}")
                    await fconn_w.set_file_record(
                        url, owner_id=user.id, file_id=f_id, file_size=file_size, 
                        permission=permission, external=True, mime_type=mime_type)
        return file_size

    async def read_file(self, url: str, start_byte = -1, end_byte = -1) -> AsyncIterable[bytes]:
        """
        Read a file from the database.
        end byte is exclusive: [start_byte, end_byte)
        """
        # The implementation is tricky, should not keep the cursor open for too long
        validate_url(url)
        async with unique_cursor() as cur:
            fconn = FileConn(cur)
            r = await fconn.get_file_record(url)
            if r is None:
                raise FileNotFoundError(f"File {url} not found")

            if r.external:
                _blob_stream = fconn.get_file_blob_external(r.file_id, start_byte=start_byte, end_byte=end_byte)
                async def blob_stream():
                    async for chunk in _blob_stream:
                        yield chunk
            else:
                blob = await fconn.get_file_blob(r.file_id, start_byte=start_byte, end_byte=end_byte)
                async def blob_stream():
                    yield blob
        ret = blob_stream()
        return ret
    
    async def read_files_bulk(
        self, urls: list[str], 
        skip_content = False, 
        op_user: Optional[UserRecord] = None, 
        ) -> dict[str, Optional[bytes]]:
        """
        A frequent use case is to read multiple files at once, 
        this method will read all files in the list and return a dict of url -> blob.
        if the file is not found, the value will be None.
        - skip_content: if True, will not read the content of the file, resulting in a dict of url -> b''

        may raise StorageExceededError if the total size of the files exceeds MAX_MEM_FILE_BYTES
        """
        for url in urls:
            validate_url(url)
        
        async with unique_cursor() as cur:
            fconn = FileConn(cur)
            file_records = await fconn.get_file_records(urls)

            if op_user is not None:
                for r in file_records:
                    if await check_path_permission(r.url, op_user, cursor=cur) >= AccessLevel.READ:
                        continue
                    is_allowed, reason = await check_file_read_permission(op_user, r, cursor=cur)
                    if not is_allowed:
                        raise PermissionDeniedError(f"Permission denied: {op_user.username} cannot read file {r.url}: {reason}")

        # first check if the files are too big
        sum_size = sum([r.file_size for r in file_records])
        if not skip_content and sum_size > MAX_MEM_FILE_BYTES:
            raise StorageExceededError(f"Unable to read files at once, total size {sum_size} exceeds {MAX_MEM_FILE_BYTES}")
        
        self.logger.debug(f"Reading {len(file_records)} files{' (skip content)' if skip_content else ''}, getting {sum_size} bytes, from {urls}")
        # read the file content
        async with unique_cursor() as cur:
            fconn = FileConn(cur)
            blobs: dict[str, bytes] = {}
            for r in file_records:
                if skip_content:
                    blobs[r.url] = b''
                    continue

                if r.external:
                    blob_iter = fconn.get_file_blob_external(r.file_id)
                    blob = b''.join([chunk async for chunk in blob_iter])
                else:
                    blob = await fconn.get_file_blob(r.file_id)
                blobs[r.url] = blob
            
            return {url: blobs.get(url, None) for url in urls}

    async def delete_file(self, url: str, op_user: Optional[UserRecord] = None) -> Optional[FileRecord]:
        validate_url(url)

        async with transaction(DeferredFileTrash) as (cur, del_man):
            fconn = FileConn(cur)
            r = await fconn.delete_file_record(url)
            if r is None:
                return None
            if op_user is not None:
                if  r.owner_id != op_user.id and \
                    await check_path_permission(r.url, op_user, cursor=cur) < AccessLevel.WRITE:
                    # will rollback
                    raise PermissionDeniedError(f"Permission denied: {op_user.username} cannot delete file {url}")
            f_id = r.file_id
            if r.external:
                await fconn.unlink_file_blob_external(f_id, blob_del_fn = del_man.schedule)
            else:
                await fconn.unlink_file_blob(f_id)
            return r
    
    async def move_file(self, old_url: str, new_url: str, op_user: Optional[UserRecord] = None):
        validate_url(old_url)
        validate_url(new_url)

        async with transaction() as cur:
            fconn = FileConn(cur)
            r = await fconn.get_file_record(old_url)
            if r is None:
                raise FileNotFoundError(f"File {old_url} not found")
            if op_user is not None:
                if await check_path_permission(old_url, op_user, cursor=cur) < AccessLevel.WRITE:
                    raise PermissionDeniedError(f"Permission denied: {op_user.username} cannot move file {old_url}")
                if await check_path_permission(new_url, op_user, cursor=cur) < AccessLevel.WRITE:
                    raise PermissionDeniedError(f"Permission denied: {op_user.username} cannot move file to {new_url}")
            await fconn.move_file(old_url, new_url, transfer_to_user=op_user.id if op_user is not None else None)

            # check user size limit if transferring ownership
            if op_user is not None:
                user_size_used = await fconn.user_size(op_user.id)
                if user_size_used > op_user.max_storage:
                    raise StorageExceededError(f"Unable to move file, user size limit exceeded: {user_size_used} > {op_user.max_storage}")

            new_mime, _ = mimetypes.guess_type(new_url)
            if not new_mime is None:
                await fconn.update_file_record(new_url, mime_type=new_mime)
    
    # not tested
    async def copy_file(self, old_url: str, new_url: str, op_user: Optional[UserRecord] = None):
        validate_url(old_url)
        validate_url(new_url)

        async with transaction() as cur:
            fconn = FileConn(cur)
            r = await fconn.get_file_record(old_url)
            if r is None:
                raise FileNotFoundError(f"File {old_url} not found")
            if op_user is not None:
                if await check_path_permission(old_url, op_user, cursor=cur) < AccessLevel.READ:
                    raise PermissionDeniedError(f"Permission denied: {op_user.username} cannot copy file {old_url}")
                if await check_path_permission(new_url, op_user, cursor=cur) < AccessLevel.WRITE:
                    raise PermissionDeniedError(f"Permission denied: {op_user.username} cannot copy file to {new_url}")
            await fconn.copy_file(old_url, new_url, user_id=op_user.id if op_user is not None else None)

            # check user size limit if transferring ownership
            if op_user is not None:
                user_size_used = await fconn.user_size(op_user.id)
                if user_size_used > op_user.max_storage:
                    raise StorageExceededError(f"Unable to copy file, user size limit exceeded: {user_size_used} > {op_user.max_storage}")
    
    async def move_dir(self, old_url: str, new_url: str, op_user: UserRecord):
        validate_url(old_url, 'dir')
        validate_url(new_url, 'dir')

        if new_url.startswith('/'):
            new_url = new_url[1:]
        if old_url.startswith('/'):
            old_url = old_url[1:]
        assert_or(old_url != new_url, InvalidPathError("Old and new path must be different"))
        assert_or(old_url.endswith('/'), InvalidPathError("Old path must end with /"))
        assert_or(new_url.endswith('/'), InvalidPathError("New path must end with /"))

        async with unique_cursor() as cur:
            if not (
                await check_path_permission(old_url, op_user, cursor=cur) >= AccessLevel.WRITE and
                await check_path_permission(new_url, op_user, cursor=cur) >= AccessLevel.WRITE
                ):
                raise PermissionDeniedError(f"Permission denied: {op_user.username} cannot move path {old_url} to {new_url}")

        async with transaction() as cur:
            fconn = FileConn(cur)
            await fconn.move_dir(old_url, new_url, op_user.id)

            # check user size limit
            user_size_used = await fconn.user_size(op_user.id)
            if user_size_used > op_user.max_storage:
                raise StorageExceededError(f"Unable to move path, user size limit exceeded: {user_size_used} > {op_user.max_storage}")
                
    
    async def copy_dir(self, old_url: str, new_url: str, op_user: UserRecord):
        validate_url(old_url, 'dir')
        validate_url(new_url, 'dir')
        
        if new_url.startswith('/'):
            new_url = new_url[1:]
        if old_url.startswith('/'):
            old_url = old_url[1:]
        assert_or(old_url != new_url, InvalidPathError("Old and new path must be different"))
        assert_or(old_url.endswith('/'), InvalidPathError("Old path must end with /"))
        assert_or(new_url.endswith('/'), InvalidPathError("New path must end with /"))

        async with unique_cursor() as cur:
            if not (
                await check_path_permission(old_url, op_user, cursor=cur) >= AccessLevel.READ and
                await check_path_permission(new_url, op_user, cursor=cur) >= AccessLevel.WRITE
                ):
                raise PermissionDeniedError(f"Permission denied: {op_user.username} cannot copy path {old_url} to {new_url}")
        
        async with transaction() as cur:
            fconn = FileConn(cur)
            await fconn.copy_dir(old_url, new_url, op_user.id)

            # check user size limit
            user_size_used = await fconn.user_size(op_user.id)
            if user_size_used > op_user.max_storage:
                raise StorageExceededError(f"Unable to copy path, user size limit exceeded: {user_size_used} > {op_user.max_storage}")

    async def __batch_unlink_file_blobs(
        self, fconn: FileConn, file_records: list[FileRecord], batch_size: int = 512,
        blob_del_fn = remove_external_blob
    ):
        # https://github.com/langchain-ai/langchain/issues/10321
        internal_ids = []
        external_ids = []
        for r in file_records:
            if r.external:
                external_ids.append(r.file_id)
            else:
                internal_ids.append(r.file_id)
        
        async def del_internal():
            for i in range(0, len(internal_ids), batch_size):
                await fconn.unlink_file_blobs([r for r in internal_ids[i:i+batch_size]])
        async def del_external():
            for i in range(0, len(external_ids), batch_size):
                await fconn.unlink_file_blobs_external([r for r in external_ids[i:i+batch_size]], blob_del_fn=blob_del_fn)
        await del_internal()
        await del_external()

    async def delete_dir(self, url: str, op_user: Optional[UserRecord] = None) -> Optional[list[FileRecord]]:
        validate_url(url, 'dir')
        if op_user is not None:
            if await check_path_permission(url, op_user) < AccessLevel.WRITE:
                raise PermissionDeniedError(f"Permission denied: {op_user.username} cannot delete path {url}")

        async with transaction(DeferredFileTrash) as (cur, del_man):
            fconn = FileConn(cur)
            records = await fconn.delete_records_by_prefix(url)
            if not records:
                return None
            await self.__batch_unlink_file_blobs(fconn, records, blob_del_fn = del_man.schedule)
            return records
    
    async def delete_user(self, u: str | int):
        async with transaction(DeferredFileTrash) as (cur, del_man):
            user = await get_user(cur, u)
            if user is None:
                raise UserNotFoundError(f"User {u} not found")

            # no new files can be added since profile deletion
            uconn = UserConn(cur)
            await uconn.delete_user(user.username)

            fconn = FileConn(cur)

            # make sure the user's directory is deleted, 
            to_del_records = await fconn.delete_records_by_prefix(user.username + '/')

            # transfer ownership of files outside the user's directory
            to_transfer_records = await fconn.list_user_file_records(user.id)
            __user_map: dict[str, UserRecord] = {}
            for r in to_transfer_records:
                r_username = r.url.split('/')[0]
                if not r_username in __user_map:
                    r_user = await uconn.get_user(r_username)
                    assert r_user is not None, f"User {r_username} not found"
                    __user_map[r_username] = r_user
                r_user = __user_map[r_username]
                await fconn.transfer_ownership(r.url, r_user.id)

            # check user size limit
            for r_user in __user_map.values():
                user_size_used = await fconn.user_size(r_user.id)
                if user_size_used > r_user.max_storage:
                    raise StorageExceededError(f"Unable to transfer files, user size limit exceeded for {r_user.username}: {user_size_used} > {r_user.max_storage}")

            self.logger.info(f"Transferred ownership of {len(to_transfer_records)} file(s) outside user {user.username}'s directory")

            # release file blobs finally
            await self.__batch_unlink_file_blobs(fconn, to_del_records, blob_del_fn=del_man.schedule)
            self.logger.info(f"Deleted user {user.username} and {len(to_del_records)} file(s) under the user's directory")
        return user
    
    async def iter_dir(self, top_url: str, urls: Optional[list[str]]) -> AsyncIterable[tuple[FileRecord, bytes | AsyncIterable[bytes]]]:
        validate_url(top_url, 'dir')
        async with unique_cursor() as cur:
            fconn = FileConn(cur)
            if urls is None:
                fcount = await fconn.count_dir_files(top_url, flat=True)
                urls = [r.url for r in (await fconn.list_dir_files(top_url, flat=True, limit=fcount))]

            for url in urls:
                if not url.startswith(top_url):
                    continue
                r = await fconn.get_file_record(url)
                if r is None:
                    continue
                f_id = r.file_id
                if r.external:
                    blob = fconn.get_file_blob_external(f_id)
                else:
                    blob = await fconn.get_file_blob(f_id)
                yield r, blob
    
    async def zip_dir_stream(self, top_url: str, op_user: Optional[UserRecord] = None) -> AsyncIterable[bytes]:
        from stat import S_IFREG
        from stream_zip import async_stream_zip, ZIP_64
        if top_url.startswith('/'):
            top_url = top_url[1:]
        
        if op_user:
            if await check_path_permission(top_url, op_user) < AccessLevel.READ:
                raise PermissionDeniedError(f"Permission denied: {op_user.username} cannot zip path {top_url}")
        
        # https://stream-zip.docs.trade.gov.uk/async-interface/
        async def data_iter():
            async for (r, blob) in self.iter_dir(top_url, None):
                rel_path = r.url[len(top_url):]
                rel_path = decode_uri_components(rel_path)
                b_iter: AsyncIterable[bytes]
                if isinstance(blob, bytes):
                    async def blob_iter(): yield blob
                    b_iter = blob_iter()    # type: ignore
                else:
                    assert isinstance(blob, AsyncIterable)
                    b_iter = blob
                yield (
                    rel_path, 
                    datetime.datetime.now(), 
                    S_IFREG | 0o600,
                    ZIP_64, 
                    b_iter
                )
        return async_stream_zip(data_iter())

    @concurrent_wrap()
    async def zip_dir(self, top_url: str, op_user: Optional[UserRecord]) -> io.BytesIO:
        if top_url.startswith('/'):
            top_url = top_url[1:]
        
        if op_user:
            if await check_path_permission(top_url, op_user) < AccessLevel.READ:
                raise PermissionDeniedError(f"Permission denied: {op_user.username} cannot zip path {top_url}")
        
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w') as zf:
            async for (r, blob) in self.iter_dir(top_url, None):
                rel_path = r.url[len(top_url):]
                rel_path = decode_uri_components(rel_path)
                if r.external:
                    assert isinstance(blob, AsyncIterable)
                    zf.writestr(rel_path, b''.join([chunk async for chunk in blob]))
                else:
                    assert isinstance(blob, bytes)
                    zf.writestr(rel_path, blob)
        buffer.seek(0)
        return buffer
