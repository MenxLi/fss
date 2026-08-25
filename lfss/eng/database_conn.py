""" Low-level database access layer.  """

from typing import Optional, Literal, overload
from collections.abc import AsyncIterable
from abc import ABC
import re, time

import urllib.parse
import asyncio

import aiosqlite, aiofiles
import aiofiles.os

from .connection_pool import transaction
from .datatype import (
    UserRecord, AccessLevel, 
    FileReadPermission, FileRecord, DirectoryRecord, PathContents, 
    FileSortKey, DirSortKey, isValidFileSortKey, isValidDirSortKey
    )
from .config import LARGE_BLOB_DIR, CHUNK_SIZE, DIR_CONFIG_FNAME
from .log import get_logger
from .utils import hash_credential, debounce_async
from .error import *

ENCODE_DIR_CONFIG_FNAME = urllib.parse.quote(DIR_CONFIG_FNAME)

# define here to avoid circular import
class DBObjectBase(ABC):
    """
    NOTE: 
    The object of this class should hold a cursor to the database. 
    The methods calling the cursor should not be called concurrently. 
    """

    logger = get_logger('database', global_instance=True)
    _cur: aiosqlite.Cursor

    def set_cursor(self, cur: aiosqlite.Cursor):
        self._cur = cur

    @property
    def cur(self)->aiosqlite.Cursor:
        if not hasattr(self, '_cur'):
            raise ValueError("Connection not set")
        return self._cur

async def remove_external_blob(file_id: str):
    if (LARGE_BLOB_DIR / file_id).exists():
        await aiofiles.os.remove(LARGE_BLOB_DIR / file_id)

class UserConn(DBObjectBase):

    def __init__(self, cur: aiosqlite.Cursor) -> None:
        super().__init__()
        self.set_cursor(cur)

    @staticmethod
    def parse_record(record) -> UserRecord:
        return UserRecord(*record)

    @staticmethod
    def escape_sqlike(url: str) -> str:
        return url.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')

    async def get_user(self, username: str) -> Optional[UserRecord]:
        await self.cur.execute("SELECT * FROM user WHERE username = ?", (username, ))
        res = await self.cur.fetchone()
        
        if res is None: return None
        return self.parse_record(res)
    
    @overload
    async def get_user_by_id(self, user_id: int, throw: Literal[True]) -> UserRecord: ...
    @overload
    async def get_user_by_id(self, user_id: int, throw: Literal[False] = False) -> Optional[UserRecord]: ...
    async def get_user_by_id(self, user_id: int, throw = False) -> Optional[UserRecord]:
        await self.cur.execute("SELECT * FROM user WHERE id = ?", (user_id, ))
        res = await self.cur.fetchone()
        if res is None:
            if throw: raise UserNotFoundError(f"User {user_id} not found")
            return None
        return self.parse_record(res)
    
    async def get_user_by_credential(self, credential: str) -> Optional[UserRecord]:
        await self.cur.execute("SELECT * FROM user WHERE credential = ?", (credential, ))
        res = await self.cur.fetchone()
        
        if res is None: return None
        return self.parse_record(res)
    
    async def create_user(
        self, username: str, password: str, is_admin: bool = False, 
        max_storage: int = 1073741824, permission: FileReadPermission = FileReadPermission.UNSET, 
        _validate = True
        ) -> int:
        def validate_username(username: str):
            assert_or(not set(username) & {'/', ':'}, InvalidInputError("Invalid username"))
            assert_or(not username.startswith('_'), InvalidInputError("Error: reserved username"))
            assert_or(not username.startswith('.'), InvalidInputError("Error: reserved username"))
            assert_or(not (len(username) > 255), InvalidInputError("Username too long"))
            assert_or(urllib.parse.quote(username) == username, InvalidInputError("Invalid username, must be URL safe"))
        if _validate:
            validate_username(username)
        self.logger.debug(f"Creating user {username}")
        credential = hash_credential(username, password)
        assert_or(await self.get_user(username) is None, InvalidDataError(f"Duplicate username: {username}"))
        await self.cur.execute("INSERT INTO user (username, credential, is_admin, max_storage, permission) VALUES (?, ?, ?, ?, ?)", (username, credential, is_admin, max_storage, permission))
        self.logger.info(f"User {username} created")
        assert self.cur.lastrowid is not None
        return self.cur.lastrowid
    
    async def query_user_expire(self, user_id: int) -> Optional[int]:
        """ Return the remaining seconds before user expire, None if no expire is set. """
        await self.cur.execute("SELECT posix_stamp FROM uexpire WHERE user_id = ?", (user_id, ))
        res = await self.cur.fetchone()
        return res[0] - int(time.time()) if res is not None else None
    
    async def set_user_expire(self, user_id: int, expire_seconds: int):
        """ Set the user to expire in `expire_seconds` seconds from now. """
        expire_time = int(time.time()) + expire_seconds
        await self.cur.execute("INSERT OR REPLACE INTO uexpire (user_id, posix_stamp) VALUES (?, ?)", (user_id, expire_time))
        self.logger.info(f"Set user {user_id} to expire in {expire_seconds} seconds")
    
    async def clear_user_expire(self, user_id: int):
        await self.cur.execute("DELETE FROM uexpire WHERE user_id = ?", (user_id, ))
        self.logger.info(f"Cleared expire for user {user_id}")
    
    async def update_user(
        self, username: str, password: Optional[str] = None, is_admin: Optional[bool] = None, 
        max_storage: Optional[int] = None, permission: Optional[FileReadPermission] = None
        ):
        assert_or(not username.startswith('_'), InvalidInputError("Error: reserved username"))
        assert_or(not ('/' in username or len(username) > 255), InvalidInputError("Invalid username"))
        assert_or(urllib.parse.quote(username) == username, InvalidInputError("Invalid username, must be URL safe"))

        current_record = await self.get_user(username)
        if current_record is None:
            raise UserNotFoundError(f"User {username} not found")

        if password is not None:
            credential = hash_credential(username, password)
        else:
            credential = current_record.credential
        
        if is_admin is None: is_admin = current_record.is_admin
        if max_storage is None: max_storage = current_record.max_storage
        if permission is None: permission = current_record.permission
        
        await self.cur.execute(
            "UPDATE user SET credential = ?, is_admin = ?, max_storage = ?, permission = ? WHERE username = ?", 
            (credential, is_admin, max_storage, int(permission), username)
            )
        self.logger.info(f"User {username} updated")
    
    async def iter_all(self) -> AsyncIterable[UserRecord]:
        await self.cur.execute("SELECT * FROM user where username NOT LIKE '.%'")
        for record in await self.cur.fetchall():
            yield self.parse_record(record)
    
    async def iter_hidden(self) -> AsyncIterable[UserRecord]:
        await self.cur.execute("SELECT * FROM user where username LIKE '.%'")
        for record in await self.cur.fetchall():
            yield self.parse_record(record)
    
    async def set_active(self, username: str):
        await self.cur.execute("UPDATE user SET last_active = CURRENT_TIMESTAMP WHERE username = ?", (username, ))
    
    async def delete_user(self, username: str):
        """ Note: this will not delete files owned by the user, please use higher level API to delete user and files together. """
        await self.cur.execute("DELETE FROM upeer WHERE src_user_id = (SELECT id FROM user WHERE username = ?) OR dst_user_id = (SELECT id FROM user WHERE username = ?)", (username, username))
        await self.cur.execute("DELETE FROM user WHERE username = ?", (username, ))
        await self.cur.execute("DELETE FROM usize WHERE user_id = (SELECT id FROM user WHERE username = ?)", (username, ))
        await self.cur.execute("DELETE FROM uexpire WHERE user_id = (SELECT id FROM user WHERE username = ?)", (username, ))
        self.logger.info(f"Delete user {username}")
    
    async def set_peer_level(self, src_user: int | str, dst_user: int | str, level: AccessLevel):
        """ 
        src_user can do [AccessLevel] to dst_user 
        if set to AccessLevel.NONE, remove the peer relation record.
        """
        assert int(level) >= AccessLevel.NONE, f"Cannot set alias level to {level}"
        if level == AccessLevel.NONE:
            match (src_user, dst_user):
                case (int(), int()):
                    await self.cur.execute("DELETE FROM upeer WHERE src_user_id = ? AND dst_user_id = ?", (src_user, dst_user))
                case (str(), str()):
                    await self.cur.execute("DELETE FROM upeer WHERE src_user_id = (SELECT id FROM user WHERE username = ?) AND dst_user_id = (SELECT id FROM user WHERE username = ?)", (src_user, dst_user))
                case (str(), int()):
                    await self.cur.execute("DELETE FROM upeer WHERE src_user_id = (SELECT id FROM user WHERE username = ?) AND dst_user_id = ?", (src_user, dst_user))
                case (int(), str()):
                    await self.cur.execute("DELETE FROM upeer WHERE src_user_id = ? AND dst_user_id = (SELECT id FROM user WHERE username = ?)", (src_user, dst_user))
                case (_, _):
                    raise ValueError("Invalid arguments")
        else:
            match (src_user, dst_user):
                case (int(), int()):
                    await self.cur.execute("INSERT OR REPLACE INTO upeer (src_user_id, dst_user_id, access_level) VALUES (?, ?, ?)", (src_user, dst_user, int(level)))
                case (str(), str()):
                    await self.cur.execute("INSERT OR REPLACE INTO upeer (src_user_id, dst_user_id, access_level) VALUES ((SELECT id FROM user WHERE username = ?), (SELECT id FROM user WHERE username = ?), ?)", (src_user, dst_user, int(level)))
                case (str(), int()):
                    await self.cur.execute("INSERT OR REPLACE INTO upeer (src_user_id, dst_user_id, access_level) VALUES ((SELECT id FROM user WHERE username = ?), ?, ?)", (src_user, dst_user, int(level)))
                case (int(), str()):
                    await self.cur.execute("INSERT OR REPLACE INTO upeer (src_user_id, dst_user_id, access_level) VALUES (?, (SELECT id FROM user WHERE username = ?), ?)", (src_user, dst_user, int(level)))
                case (_, _):
                    raise ValueError("Invalid arguments")
    
    async def query_peer_level(self, src_user_id: int, dst_user_id: int) -> AccessLevel:
        """ src_user can do [AliasLevel] to dst_user """
        if src_user_id == dst_user_id:
            return AccessLevel.ALL
        await self.cur.execute("SELECT access_level FROM upeer WHERE src_user_id = ? AND dst_user_id = ?", (src_user_id, dst_user_id))
        res = await self.cur.fetchone()
        if res is None:
            return AccessLevel.NONE
        return AccessLevel(res[0])
    
    async def list_all_users(self) -> list[UserRecord]:
        return [u async for u in self.iter_all()]
    
    async def list_users(
        self, 
        username_filter: Optional[str] = None,
        include_virtual: bool = False,
        order_by: Literal['username', 'create_time', 'is_admin', 'last_active'] = 'create_time',
        order_desc: bool = False,
        offset: int = 0,
        limit: int = 1000
    ) -> list[UserRecord]:
        query = "SELECT * FROM user WHERE 1=1"
        params = []
        
        if not include_virtual:
            query += " AND username NOT LIKE '.%'"
            
        if username_filter:
            query += " AND username LIKE ? ESCAPE '\\'"
            params.append(f"%{self.escape_sqlike(username_filter)}%")
            
        if order_by not in ['username', 'create_time', 'is_admin', 'last_active']:
            order_by = 'create_time'
            
        query += f" ORDER BY {order_by} {'DESC' if order_desc else 'ASC'}"
        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        await self.cur.execute(query, tuple(params))
        res = await self.cur.fetchall()
        return [self.parse_record(r) for r in res]
    
    async def list_admin_users(self) -> list[UserRecord]:
        await self.cur.execute("SELECT * FROM user WHERE is_admin = 1")
        res = await self.cur.fetchall()
        return [self.parse_record(r) for r in res]
    
    async def list_peer_users(self, user: int | str, level: AccessLevel, incoming = False) -> list[UserRecord]:
        """
        if not incoming:
            List all users that user can do [AliasLevel] to, with level >= level, 
        else:
            List all users that can do [AliasLevel] to user, with level >= level

        Note: the returned list does not include the user and is not apporiate for admin (who has all permissions for all users)
        """
        assert int(level) > AccessLevel.NONE, f"Invalid level, {level}"
        aim_field = 'src_user_id' if incoming else 'dst_user_id'
        query_field = 'dst_user_id' if incoming else 'src_user_id'
    
        match user:
            case int():
                await self.cur.execute(f"""
                    SELECT * FROM user WHERE id IN (
                        SELECT {aim_field} FROM upeer WHERE {query_field} = ? AND access_level >= ?
                    )
                """, (user, int(level)))
            case str():
                await self.cur.execute(f"""
                    SELECT * FROM user WHERE id IN (
                        SELECT {aim_field} FROM upeer WHERE {query_field} = (SELECT id FROM user WHERE username = ?) AND access_level >= ?
                    )
                """, (user, int(level)))
            case _:
                raise ValueError("Invalid arguments")
        res = await self.cur.fetchall()
        return [self.parse_record(r) for r in res]

class FileConn(DBObjectBase):

    def __init__(self, cur: aiosqlite.Cursor) -> None:
        super().__init__()
        self.set_cursor(cur)

    @staticmethod
    def parse_record(record) -> FileRecord:
        return FileRecord(*record)
    
    @staticmethod
    def escape_sqlike(url: str) -> str:
        """ Escape a url for use in SQL LIKE clause (The % and _ characters) """
        return url.replace('%', r'\%').replace('_', r'\_')
    
    @overload
    async def get_file_record(self, url: str, throw: Literal[True]) -> FileRecord: ...
    @overload
    async def get_file_record(self, url: str, throw: Literal[False] = False) -> Optional[FileRecord]: ...
    async def get_file_record(self, url: str, throw = False):
        cursor = await self.cur.execute("SELECT * FROM fmeta WHERE url = ?", (url, ))
        res = await cursor.fetchone()
        if res is None:
            if throw: raise FileNotFoundError(f"File {url} not found")
            return None
        return self.parse_record(res)
    
    async def get_file_records(self, urls: list[str]) -> list[FileRecord]:
        """
        Get all file records with the given urls, only urls in the database will be returned. 
        If the urls are not in the database, they will be ignored.
        """
        await self.cur.execute("SELECT * FROM fmeta WHERE url IN ({})".format(','.join(['?'] * len(urls))), urls)
        res = await self.cur.fetchall()
        if res is None:
            return []
        return [self.parse_record(r) for r in res]
    
    async def list_root_dirs(self, *usernames: str, skim = False) -> list[DirectoryRecord]:
        """
        Efficiently list users' directories, if usernames is empty, list all users' directories.
        """
        if not usernames:
            # list all users
            await self.cur.execute("SELECT username FROM user")
            res = await self.cur.fetchall()
            dirnames = [u[0] + '/' for u in res]
            dirs = [await self.get_dir_record(u) for u in dirnames] if not skim else [DirectoryRecord(u) for u in dirnames]
            return dirs
        else:
            # list specific users
            dirnames = [uname + '/' for uname in usernames]
            dirs = [await self.get_dir_record(u) for u in dirnames] if not skim else [DirectoryRecord(u) for u in dirnames]
            return dirs
    
    async def count_path_dirs(self, url: str):
        if not url.endswith('/'): url += '/'
        if url == '/': url = ''
        cursor = await self.cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT SUBSTR( 
                url, LENGTH(?) + 1, 
                INSTR(SUBSTR(url, LENGTH(?) + 1), '/')
                ) AS dirname
            FROM fmeta WHERE url LIKE ? ESCAPE '\\' AND dirname != ''
        )
        """, (url, url, self.escape_sqlike(url) + '%'))
        res = await cursor.fetchone()
        assert res is not None, "Error: count_path_dirs"
        return res[0]

    async def list_path_dirs(
        self, url: str, 
        offset: int = 0, limit: int = 10_000, 
        order_by: DirSortKey = '', order_desc: bool = False,
        skim: bool = True
        ) -> list[DirectoryRecord]:
        if not isValidDirSortKey(order_by):
            raise ValueError(f"Invalid order_by ({order_by})")

        if not url.endswith('/'): url += '/'
        if url == '/': url = ''

        sql_qury = """
            SELECT DISTINCT SUBSTR(
                url, 
                1 + LENGTH(?),
                INSTR(SUBSTR(url, 1 + LENGTH(?)), '/')
            ) AS dirname 
            FROM fmeta WHERE url LIKE ? ESCAPE '\\' AND dirname != ''
        """ \
        + (f"ORDER BY {order_by} {'DESC' if order_desc else 'ASC'}" if order_by else '') \
        + " LIMIT ? OFFSET ?"
        cursor = await self.cur.execute(sql_qury, (url, url, self.escape_sqlike(url) + '%', limit, offset))
        res = await cursor.fetchall()
        dirs_str = [r[0] for r in res]
        async def get_dir(dir_url):
            if skim:
                return DirectoryRecord(dir_url)
            else:
                return await self.get_dir_record(dir_url)
        dirs = [await get_dir(url + d) for d in dirs_str]
        return dirs
    
    async def is_dir_exist(self, url: str) -> bool:
        if not url.endswith('/'): url += '/'
        cursor = await self.cur.execute("SELECT 1 FROM fmeta WHERE url LIKE ? ESCAPE '\\' LIMIT 1", (self.escape_sqlike(url) + '%', ))
        res = await cursor.fetchone()
        return res is not None
    
    async def count_dir_files(self, url: str, flat: bool = False):
        if not url.endswith('/'): url += '/'
        if url == '/': url = ''
        if flat:
            cursor = await self.cur.execute(
                "SELECT COUNT(*) FROM fmeta WHERE url LIKE ? ESCAPE '\\'", 
                (self.escape_sqlike(url) + '%', )
                )
        else:
            cursor = await self.cur.execute(
                "SELECT COUNT(*) FROM fmeta WHERE url LIKE ? ESCAPE '\\' AND url NOT LIKE ? ESCAPE '\\'", 
                (self.escape_sqlike(url) + '%', self.escape_sqlike(url) + '%/%')
                )
        res = await cursor.fetchone()
        assert res is not None, "Error: count_path_files"
        return res[0]

    async def list_dir_files(
        self, url: str, 
        offset: int = 0, limit: int = 10_000, 
        order_by: FileSortKey = '', order_desc: bool = False,
        flat: bool = False, 
        ) -> list[FileRecord]:
        if not isValidFileSortKey(order_by):
            raise ValueError(f"Invalid order_by {order_by}")

        if not url.endswith('/'): url += '/'
        if url == '/': url = ''

        sql_query = "SELECT * FROM fmeta WHERE url LIKE ? ESCAPE '\\'"
        if not flat: sql_query += " AND url NOT LIKE ? ESCAPE '\\'"
        if order_by: sql_query += f" ORDER BY {order_by} {'DESC' if order_desc else 'ASC'}"
        sql_query += " LIMIT ? OFFSET ?"
        if flat:
            cursor = await self.cur.execute(sql_query, (self.escape_sqlike(url) + '%', limit, offset))
        else:
            cursor = await self.cur.execute(sql_query, (self.escape_sqlike(url) + '%', self.escape_sqlike(url) + '%/%', limit, offset))
        res = await cursor.fetchall()
        files = [self.parse_record(r) for r in res]
        return files
    
    # DEPRECATED
    async def list_path(self, url: str) -> PathContents:
        """
        List all files and directories under the given path.  
        This method is a handy way file browsing, but has limitaions:
        - It does not support pagination
        - It does not support sorting
        - It cannot flatten directories
        - It cannot list directories with details
        """
        MAX_ITEMS = 10_000
        dir_count = await self.count_path_dirs(url)
        file_count = await self.count_dir_files(url, flat=False)
        if dir_count + file_count > MAX_ITEMS:
            raise TooManyItemsError("Too many items, please paginate")
        return PathContents(
            dirs = await self.list_path_dirs(url, skim=True, limit=MAX_ITEMS),
            files = await self.list_dir_files(url, flat=False, limit=MAX_ITEMS)
            )
    
    async def get_dir_record(self, url: str) -> DirectoryRecord:
        """
        Get the full record of a directory, including size, create_time, update_time, access_time etc.
        """
        assert url.endswith('/'), "Path must end with /"
        cursor = await self.cur.execute("""
            SELECT MIN(create_time) as create_time, 
                MAX(create_time) as update_time, 
                MAX(access_time) as access_time, 
                COUNT(*) as n_files
            FROM fmeta 
            WHERE url LIKE ? ESCAPE '\\'
        """, (self.escape_sqlike(url) + '%', ))
        result = await cursor.fetchone()
        if result is None or any(val is None for val in result):
            raise PathNotFoundError(f"Path {url} not found")
        create_time, update_time, access_time, n_files = result
        p_size = await self.path_size(url, include_subpath=True)
        return DirectoryRecord(url, p_size, create_time=create_time, update_time=update_time, access_time=access_time, n_files=n_files)
    
    async def user_size(self, user_id: int) -> int:
        cursor = await self.cur.execute("SELECT size FROM usize WHERE user_id = ?", (user_id, ))
        res = await cursor.fetchone()
        if res is None: return 0
        return res[0]
    async def _user_size_inc(self, user_id: int, inc: int):
        self.logger.debug(f"Increasing user {user_id} size by {inc}")
        await self.cur.execute("INSERT OR REPLACE INTO usize (user_id, size) VALUES (?, COALESCE((SELECT size FROM usize WHERE user_id = ?), 0) + ?)", (user_id, user_id, inc))
    async def _user_size_dec(self, user_id: int, dec: int):
        self.logger.debug(f"Decreasing user {user_id} size by {dec}")
        await self.cur.execute("INSERT OR REPLACE INTO usize (user_id, size) VALUES (?, COALESCE((SELECT size FROM usize WHERE user_id = ?), 0) - ?)", (user_id, user_id, dec))
    
    async def path_size(self, url: str, include_subpath = False) -> int:
        if not url.endswith('/'):
            url += '/'
        if not include_subpath:
            cursor = await self.cur.execute(
                "SELECT SUM(file_size) FROM fmeta WHERE url LIKE ? ESCAPE '\\' AND url NOT LIKE ? ESCAPE '\\'",
                (self.escape_sqlike(url) + '%', self.escape_sqlike(url) + '%/%')
                )
            res = await cursor.fetchone()
        else:
            cursor = await self.cur.execute(
                "SELECT SUM(file_size) FROM fmeta WHERE url LIKE ? ESCAPE '\\'",
                (self.escape_sqlike(url) + '%', )
                )
            res = await cursor.fetchone()
        assert res is not None
        return res[0] or 0
    
    async def update_file_record(
        self, url, 
        permission: Optional[FileReadPermission] = None, 
        mime_type: Optional[str] = None
        ):
        if permission is not None:
            await self.cur.execute("UPDATE fmeta SET permission = ? WHERE url = ?", (int(permission), url))
        if mime_type is not None:
            await self.cur.execute("UPDATE fmeta SET mime_type = ? WHERE url = ?", (mime_type, url))
        self.logger.info(f"Updated file {url}")
    
    async def set_file_record(
        self, url: str, 
        owner_id: int, 
        file_id:str, 
        file_size: int, 
        permission: FileReadPermission, 
        external: bool, 
        mime_type: str
        ):
        self.logger.debug(f"Creating fmeta {url}: permission={permission}, owner_id={owner_id}, file_id={file_id}, file_size={file_size}, external={external}, mime_type={mime_type}")
        if permission is None:
            permission = FileReadPermission.UNSET
        assert owner_id is not None and file_id is not None and file_size is not None and external is not None
        await self.cur.execute(
            "INSERT INTO fmeta (url, owner_id, file_id, file_size, permission, external, mime_type) VALUES (?, ?, ?, ?, ?, ?, ?)", 
            (url, owner_id, file_id, file_size, int(permission), external, mime_type)
            )
        await self._user_size_inc(owner_id, file_size)
        self.logger.info(f"File {url} created")

    async def copy_file(self, old_url: str, new_url: str, user_id: Optional[int] = None):
        """
        Copy file from old_url to new_url, 
        if user_id is None, will not change the owner_id of the file. Otherwise, will change the owner_id to user_id.
        """
        old = await self.get_file_record(old_url)
        if old is None:
            raise FileNotFoundError(f"File {old_url} not found")
        new_exists = await self.get_file_record(new_url)
        if new_exists is not None:
            raise FileExistsError(f"File {new_url} already exists")
        user_id = old.owner_id if user_id is None else user_id
        await self.cur.execute(
            "INSERT INTO fmeta (url, owner_id, file_id, file_size, permission, external, mime_type) VALUES (?, ?, ?, ?, ?, ?, ?)", 
            (new_url, user_id, old.file_id, old.file_size, old.permission, old.external, old.mime_type)
            )
        await self.cur.execute("INSERT OR REPLACE INTO dupcount (file_id, count) VALUES (?, COALESCE((SELECT count FROM dupcount WHERE file_id = ?), 0) + 1)", (old.file_id, old.file_id))
        await self._user_size_inc(user_id, old.file_size)
        self.logger.info(f"Copied file {old_url} to {new_url}")
    
    async def copy_dir(self, old_url: str, new_url: str, user_id: Optional[int] = None):
        """
        Copy all files under old_url to new_url, 
        if user_id is None, will not change the owner_id of the files. Otherwise, will change the owner_id to user_id.
        will skip files ending with '/{DIR_CONFIG_FNAME}' (directory config files)
        """
        assert_or(old_url.endswith('/'), InvalidInputError("Old path must end with /"))
        assert_or(new_url.endswith('/'), InvalidInputError("New path must end with /"))
        cursor = await self.cur.execute(
            "SELECT * FROM fmeta WHERE url LIKE ? ESCAPE '\\' AND url NOT LIKE ? ESCAPE '\\'",
            (self.escape_sqlike(old_url) + '%', '%/' + self.escape_sqlike(ENCODE_DIR_CONFIG_FNAME))
            )
        res = await cursor.fetchall()
        for r in res:
            old_record = FileRecord(*r)
            new_r = new_url + old_record.url[len(old_url):]
            if await (await self.cur.execute("SELECT url FROM fmeta WHERE url = ?", (new_r, ))).fetchone() is not None:
                raise FileExistsError(f"File {new_r} already exists")
            user_id = old_record.owner_id if user_id is None else user_id
            await self.cur.execute(
                "INSERT INTO fmeta (url, owner_id, file_id, file_size, permission, external, mime_type) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                (new_r, user_id, old_record.file_id, old_record.file_size, old_record.permission, old_record.external, old_record.mime_type)
                )
            await self.cur.execute("INSERT OR REPLACE INTO dupcount (file_id, count) VALUES (?, COALESCE((SELECT count FROM dupcount WHERE file_id = ?), 0) + 1)", (old_record.file_id, old_record.file_id))
            await self._user_size_inc(user_id, old_record.file_size)
        self.logger.info(f"Copied path {old_url} to {new_url}")
    
    async def move_file(self, old_url: str, new_url: str, transfer_to_user: Optional[int] = None):
        old = await self.get_file_record(old_url)
        if old is None:
            raise FileNotFoundError(f"File {old_url} not found")
        new_exists = await self.get_file_record(new_url)
        if new_exists is not None:
            raise FileExistsError(f"File {new_url} already exists")
        await self.cur.execute(
            "UPDATE fmeta SET url = ?, owner_id = ?, create_time = CURRENT_TIMESTAMP WHERE url = ?", 
            (new_url, old.owner_id if transfer_to_user is None else transfer_to_user, old_url)
        )
        if transfer_to_user is not None and transfer_to_user != old.owner_id:
            await self._user_size_dec(old.owner_id, old.file_size)
            await self._user_size_inc(transfer_to_user, old.file_size)
        self.logger.info(f"Moved file {old_url} to {new_url}")
    
    async def transfer_ownership(self, url: str, new_owner: int):
        old = await self.get_file_record(url)
        if old is None:
            raise FileNotFoundError(f"File {url} not found")
        if new_owner == old.owner_id:
            return
        await self.cur.execute("UPDATE fmeta SET owner_id = ? WHERE url = ?", (new_owner, url))
        await self._user_size_dec(old.owner_id, old.file_size)
        await self._user_size_inc(new_owner, old.file_size)
        self.logger.info(f"Transferred ownership of file {url} from user {old.owner_id} to user {new_owner}")
    
    async def move_dir(self, old_url: str, new_url: str, transfer_to_user: Optional[int] = None):
        """
        will skip files ending with '/{DIR_CONFIG_FNAME}' (directory config files)
        """
        assert_or(old_url.endswith('/'), InvalidInputError("Old path must end with /"))
        assert_or(new_url.endswith('/'), InvalidInputError("New path must end with /"))
        cursor = await self.cur.execute(
            "SELECT url, owner_id, file_size FROM fmeta WHERE url LIKE ? ESCAPE '\\' AND url NOT LIKE ? ESCAPE '\\'",
            (self.escape_sqlike(old_url) + '%', '%/' + self.escape_sqlike(ENCODE_DIR_CONFIG_FNAME))
            )
        res = await cursor.fetchall()
        for r in res:
            r_url, r_user, r_size = r
            new_url_full = new_url + r_url[len(old_url):]
            if await (await self.cur.execute("SELECT url FROM fmeta WHERE url = ?", (new_url_full, ))).fetchone():
                self.logger.error(f"File {new_url_full} already exists on move path: {old_url} -> {new_url}")
                raise FileDuplicateError(f"File {new_url_full} already exists")
            await self.cur.execute(
                "UPDATE fmeta SET url = ?, owner_id = ?, create_time = CURRENT_TIMESTAMP WHERE url = ?", 
                (new_url_full, r_user if transfer_to_user is None else transfer_to_user, r_url)
                )
            if transfer_to_user is not None and transfer_to_user != r_user:
                await self._user_size_dec(r_user, r_size)
                await self._user_size_inc(transfer_to_user, r_size)
    
    async def log_access(self, url: str):
        await self.cur.execute("UPDATE fmeta SET access_time = CURRENT_TIMESTAMP WHERE url = ?", (url, ))
    
    async def delete_file_record(self, url: str) -> Optional[FileRecord]:
        res = await self.cur.execute("DELETE FROM fmeta WHERE url = ? RETURNING *", (url, ))
        row = await res.fetchone()
        if row is None:
            raise FileNotFoundError(f"File {url} not found")
        file_record = FileRecord(*row)
        await self._user_size_dec(file_record.owner_id, file_record.file_size)
        self.logger.info(f"Deleted fmeta {url}")
        return file_record
    
    async def list_user_file_records(self, owner_id: int) -> list[FileRecord]:
        """ list all records with owner_id """
        cursor = await self.cur.execute("SELECT * FROM fmeta WHERE owner_id = ?", (owner_id, ))
        res = await cursor.fetchall()
        return [self.parse_record(r) for r in res]

    async def delete_records_by_prefix(self, path: str) -> list[FileRecord]:
        """ Delete all records with url starting with path """
        # update user size
        cursor = await self.cur.execute(
            "SELECT DISTINCT owner_id FROM fmeta WHERE url LIKE ? ESCAPE '\\'",
            (self.escape_sqlike(path) + '%', )
            )
        res = await cursor.fetchall()
        for r in res:
            cursor = await self.cur.execute(
                "SELECT SUM(file_size) FROM fmeta WHERE owner_id = ? AND url LIKE ? ESCAPE '\\'", 
                (r[0], self.escape_sqlike(path) + '%')
                )
            size = await cursor.fetchone()
            if size is not None:
                await self._user_size_dec(r[0], size[0])
        
        # if any new records are created here, the size update may be inconsistent
        # but it's not a big deal... we should have only one writer
        
        res = await self.cur.execute("DELETE FROM fmeta WHERE url LIKE ? ESCAPE '\\' RETURNING *", (self.escape_sqlike(path) + '%', ))
        all_f_rec = await res.fetchall()
        self.logger.info(f"Deleted {len(all_f_rec)} file(s) for path {path}") # type: ignore
        return [self.parse_record(r) for r in all_f_rec]
    
    async def set_file_blob(self, file_id: str, blob: bytes):
        await self.cur.execute("INSERT INTO blobs.fdata (file_id, data) VALUES (?, ?)", (file_id, blob))
    
    @staticmethod
    async def set_file_blob_external(file_id: str, stream: AsyncIterable[bytes])->int:
        size_sum = 0
        try:
            async with aiofiles.open(LARGE_BLOB_DIR / file_id, 'wb') as f:
                async for chunk in stream:
                    size_sum += len(chunk)
                    await f.write(chunk)
        except Exception as e:
            if (LARGE_BLOB_DIR / file_id).exists():
                await aiofiles.os.remove(LARGE_BLOB_DIR / file_id)
            raise
        return size_sum
    
    async def get_file_blob(self, file_id: str, start_byte = -1, end_byte = -1) -> bytes:
        cursor = await self.cur.execute("SELECT data FROM blobs.fdata WHERE file_id = ?", (file_id, ))
        res = await cursor.fetchone()
        if res is None:
            raise FileNotFoundError(f"File {file_id} not found")
        blob = res[0]
        match (start_byte, end_byte):
            case (-1, -1):
                return blob
            case (s, -1):
                return blob[s:]
            case (-1, e):
                return blob[:e]
            case (s, e):
                return blob[s:e]
    
    @staticmethod
    async def get_file_blob_external(file_id: str, start_byte = -1, end_byte = -1) -> AsyncIterable[bytes]:
        assert (LARGE_BLOB_DIR / file_id).exists(), f"File {file_id} not found"
        async with aiofiles.open(LARGE_BLOB_DIR / file_id, 'rb') as f:
            if start_byte >= 0:
                await f.seek(start_byte)
            if end_byte >= 0:
                while True:
                    head_ptr = await f.tell()
                    if head_ptr >= end_byte:
                        break
                    chunk = await f.read(min(CHUNK_SIZE, end_byte - head_ptr))
                    if not chunk: break
                    yield chunk
            else:
                while True:
                    chunk = await f.read(CHUNK_SIZE)
                    if not chunk: break
                    yield chunk
    
    async def unlink_file_blob_external(self, file_id: str, blob_del_fn = remove_external_blob):
        # first check if the file has duplication
        cursor = await self.cur.execute("SELECT count FROM dupcount WHERE file_id = ?", (file_id, ))
        res = await cursor.fetchone()
        if res is not None and res[0] > 0:
            await self.cur.execute("UPDATE dupcount SET count = count - 1 WHERE file_id = ?", (file_id, ))
            return

        # finally delete the file and the duplication count
        await blob_del_fn(file_id)
        await self.cur.execute("DELETE FROM dupcount WHERE file_id = ?", (file_id, ))
    
    async def unlink_file_blob(self, file_id: str):
        # first check if the file has duplication
        cursor = await self.cur.execute("SELECT count FROM dupcount WHERE file_id = ?", (file_id, ))
        res = await cursor.fetchone()
        if res is not None and res[0] > 0:
            await self.cur.execute("UPDATE dupcount SET count = count - 1 WHERE file_id = ?", (file_id, ))
            return

        # finally delete the file and the duplication count
        await self.cur.execute("DELETE FROM blobs.fdata WHERE file_id = ?", (file_id, ))
        await self.cur.execute("DELETE FROM dupcount WHERE file_id = ?", (file_id, ))
    
    async def _group_del(self, file_ids_all: list[str]):
        """
        The file_ids_all may contain duplication, 
        yield tuples of unique (to_del_ids, to_dec_ids) for each iteration, 
        every iteration should unlink one copy of the files, repeat until all re-occurrence in the input list are removed.
        """
        async def check_dup(file_ids: set[str]):
            cursor = await self.cur.execute("SELECT file_id FROM dupcount WHERE file_id IN ({}) AND count > 0".format(','.join(['?'] * len(file_ids))), tuple(file_ids))
            res = await cursor.fetchall()
            to_dec_ids = [r[0] for r in res]
            to_del_ids = list(file_ids - set(to_dec_ids))
            return to_del_ids, to_dec_ids
        # gather duplication from all file_ids
        fid_occurrence = {}
        for file_id in file_ids_all:
            fid_occurrence[file_id] = fid_occurrence.get(file_id, 0) + 1
        while fid_occurrence:
            to_del_ids, to_dec_ids = await check_dup(set(fid_occurrence.keys()))
            for file_id in to_del_ids:
                del fid_occurrence[file_id]
            for file_id in to_dec_ids:
                fid_occurrence[file_id] -= 1
                if fid_occurrence[file_id] == 0:
                    del fid_occurrence[file_id]
            yield (to_del_ids, to_dec_ids)
    
    async def unlink_file_blobs(self, file_ids: list[str]):
        async for (to_del_ids, to_dec_ids) in self._group_del(file_ids):
            # delete the only copy
            await self.cur.execute("DELETE FROM blobs.fdata WHERE file_id IN ({})".format(','.join(['?'] * len(to_del_ids))), to_del_ids)
            await self.cur.execute("DELETE FROM dupcount WHERE file_id IN ({})".format(','.join(['?'] * len(to_del_ids))), to_del_ids)
            # decrease duplication count
            await self.cur.execute("UPDATE dupcount SET count = count - 1 WHERE file_id IN ({})".format(','.join(['?'] * len(to_dec_ids))), to_dec_ids)
    
    async def unlink_file_blobs_external(self, file_ids: list[str], blob_del_fn = remove_external_blob):
        async for (to_del_ids, to_dec_ids) in self._group_del(file_ids):
            # delete the only copy
            await asyncio.gather(*(
                [blob_del_fn(file_id) for file_id in to_del_ids] + 
                [self.cur.execute("DELETE FROM dupcount WHERE file_id = ?", (file_id, )) for file_id in to_del_ids]
                ))
            # decrease duplication count
            await self.cur.execute("UPDATE dupcount SET count = count - 1 WHERE file_id IN ({})".format(','.join(['?'] * len(to_dec_ids))), to_dec_ids)
        

_log_active_queue = []
_log_active_lock = asyncio.Lock()
@debounce_async()
async def _set_all_active():
    async with transaction() as conn:
        uconn = UserConn(conn)
        async with _log_active_lock:
            for u in _log_active_queue:
                await uconn.set_active(u)
            _log_active_queue.clear()
async def delayed_log_activity(username: str):
    async with _log_active_lock:
        _log_active_queue.append(username)
    await _set_all_active()

_log_access_queue = []
_log_access_lock = asyncio.Lock()
@debounce_async()
async def _log_all_access():
    async with transaction() as conn:
        fconn = FileConn(conn)
        async with _log_access_lock:
            for r in _log_access_queue:
                await fconn.log_access(r)
            _log_access_queue.clear()
async def delayed_log_access(url: str):
    async with _log_access_lock:
        _log_access_queue.append(url)
    await _log_all_access()

_validate_url_prohibited_regex = re.compile(
        r"^[/_.]",              # start with / or _ or .
    )
_validate_url_prohibited_part_regex = re.compile(
    "|".join([
        r"^\s*\.+\s*$",       # dot path
        "[{}]".format("".join(re.escape(c) for c in ('/', "\\", "'", '"', "*"))), # prohibited characters
    ])
)
def validate_url(url: str, utype: Literal['file', 'dir'] = 'file'):
    """ 
    Check if a path is valid. The input path is considered url safe 
    (url should not start with /)
    """
    if len(url) > 1024: 
        raise InvalidPathError(f"URL too long: {url}")

    is_valid = _validate_url_prohibited_regex.search(url) is None
    if not is_valid:    # early return, no need to check further
        raise InvalidPathError(f"Invalid URL: {url}")

    url_sp = url.split('/')
    for i, part in enumerate(url_sp):
        if i != len(url_sp) - 1 and part == '':
            is_valid = False
            break
        if _validate_url_prohibited_part_regex.search(urllib.parse.unquote(part)):
            is_valid = False
            break

    if utype == 'file': is_valid = is_valid and not url.endswith('/')
    else: is_valid = is_valid and url.endswith('/')

    if not is_valid: 
        raise InvalidPathError(f"Invalid URL: {url}")