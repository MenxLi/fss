""" WebDAV service """

from fastapi import Request, Response, Depends, HTTPException
import time, uuid, os
import aiosqlite
import asyncio
from typing import Literal, Optional
import xml.etree.ElementTree as ET
from ..eng.connection_pool import unique_cursor
from ..eng.error import *
from ..eng.config import DATA_HOME, DEBUG_MODE
from ..eng.datatype import UserRecord, FileRecord, DirectoryRecord, AccessLevel
from ..eng.database import FileConn, UserConn, check_path_permission
from ..eng.utils import ensure_uri_components, decode_uri_components, format_last_modified
from .app_base import *
from .common_impl import copy_impl

LOCK_DB_PATH = DATA_HOME / "lock.db"
MKDIR_PLACEHOLDER = ".lfss_keep"
DAV_NS = "DAV:"

# at the beginning of the service, remove the lock database
try: os.remove(LOCK_DB_PATH)
except Exception: ...

ET.register_namespace("d", DAV_NS)  # Register the default namespace
ptype = Literal["file", "dir", None]
async def eval_path(path: str) -> tuple[ptype, str, Optional[FileRecord | DirectoryRecord]]:
    """
    Evaluate the type of the path, 
    the return value is a uri-safe string,
    return (ptype, lfss_path, record)

    lfss_path is the path recorded in the database, 
        it should not start with /, 
        and should end with / if it is a directory, otherwise it is a file
    record is the FileRecord or DirectoryRecord object, it is None if the path does not exist
    """
    path = decode_uri_components(path)
    if "://" in path:
        if not path.startswith("http://") and not path.startswith("https://"):
            raise HTTPException(status_code=400, detail="Bad Request, unsupported protocol")
        # pop the protocol part, host part, and port part
        path = path.split("/", 3)[-1]
        route_prefix = router_dav.prefix
        if route_prefix.startswith("/"): route_prefix = route_prefix[1:]
        assert path.startswith(route_prefix), "Path should start with the route prefix, got: " + path
        path = path[len(route_prefix):]

    path = ensure_uri_components(path)
    if path.startswith("/"): path = path[1:]

    # path now is url-safe and without leading slash
    if path.endswith("/"):
        lfss_path = path
        dir_path_sp = path.split("/")
        if len(dir_path_sp) > 2:
            async with unique_cursor() as c:
                fconn = FileConn(c)
                if await fconn.count_dir_files(path, flat=True) == 0:
                    return None, lfss_path, None
                return "dir", lfss_path, await fconn.get_dir_record(path)
        else:
            # test if its a user's root directory
            assert len(dir_path_sp) == 2
            username = path.split("/")[0]
            async with unique_cursor() as c:
                uconn = UserConn(c)
                u = await uconn.get_user(username)
                if u is None: 
                    return None, lfss_path, None
                return "dir", lfss_path, DirectoryRecord(lfss_path)

    # may be root directory
    if path == "": 
        return "dir", "", DirectoryRecord("")

    # not end with /, check if it is a file
    async with unique_cursor() as c:
        res = await FileConn(c).get_file_record(path)
        if res:
            lfss_path = path
            return "file", lfss_path, res
    
    async with unique_cursor() as c:
        lfss_path = path + "/"
        fconn = FileConn(c)
        if await fconn.count_dir_files(lfss_path) > 0:
            return "dir", lfss_path, await fconn.get_dir_record(lfss_path)
    
    return None, path, None

lock_table_create_sql = """
CREATE TABLE IF NOT EXISTS locks (
    path TEXT PRIMARY KEY,
    user TEXT,
    token TEXT,
    depth TEXT,
    timeout float,
    lock_time float
);
"""
async def lock_path(user: UserRecord, p: str, token: str, depth: str, timeout: int = 1800):
    async with aiosqlite.connect(LOCK_DB_PATH) as conn:
        await conn.execute("BEGIN EXCLUSIVE")
        await conn.execute(lock_table_create_sql)
        async with conn.execute("SELECT user, timeout, lock_time FROM locks WHERE path=?", (p,)) as cur:
            row = await cur.fetchone()
            if row:
                user_, timeout_, lock_time_ = row
                curr_time = time.time()
                if timeout > 0 and curr_time - lock_time_ < timeout_:
                    raise FileLockedError(f"File is locked (by {user_}) [{p}]")
            await cur.execute("INSERT OR REPLACE INTO locks VALUES (?, ?, ?, ?, ?, ?)", (p, user.username, token, depth, timeout, time.time()))
            await conn.commit()
async def unlock_path(user: UserRecord, p: str, token: str):
    async with aiosqlite.connect(LOCK_DB_PATH) as conn:
        await conn.execute("BEGIN EXCLUSIVE")
        await conn.execute(lock_table_create_sql)
        async with conn.execute("SELECT user, token FROM locks WHERE path=?", (p,)) as cur:
            row = await cur.fetchone()
            if not row: return
            user_, token_ = row
            if user_ != user.username or token_ != token:
                raise FileLockedError(f"Failed to unlock file [{p}] with token {token}")
            await cur.execute("DELETE FROM locks WHERE path=?", (p,))
            await conn.commit()
async def query_lock_element(p: str, top_el_name: str = f"{{{DAV_NS}}}lockinfo") -> Optional[ET.Element]:
    async with aiosqlite.connect(LOCK_DB_PATH) as conn:
        await conn.execute("BEGIN EXCLUSIVE")
        await conn.execute(lock_table_create_sql)
        async with conn.execute("SELECT user, token, depth, timeout, lock_time FROM locks WHERE path=?", (p,)) as cur:
            row = await cur.fetchone()
            if not row: return None
            curr_time = time.time()
            username, token, depth, timeout, lock_time = row
            if timeout > 0 and curr_time - lock_time > timeout:
                await cur.execute("DELETE FROM locks WHERE path=?", (p,))
                await conn.commit()
                return None
        lock_info = ET.Element(top_el_name)
        locktype = ET.SubElement(lock_info, f"{{{DAV_NS}}}locktype")
        ET.SubElement(locktype, f"{{{DAV_NS}}}write")
        lockscope = ET.SubElement(lock_info, f"{{{DAV_NS}}}lockscope")
        ET.SubElement(lockscope, f"{{{DAV_NS}}}exclusive")
        owner = ET.SubElement(lock_info, f"{{{DAV_NS}}}owner")
        owner.text = username
        depth_el = ET.SubElement(lock_info, f"{{{DAV_NS}}}depth")
        depth_el.text = depth
        timeout = ET.SubElement(lock_info, f"{{{DAV_NS}}}timeout")
        timeout.text = f"Second-{timeout}"
        locktoken = ET.SubElement(lock_info, f"{{{DAV_NS}}}locktoken")
        href = ET.SubElement(locktoken, f"{{{DAV_NS}}}href")
        href.text = f"{token}"
        return lock_info

async def create_file_xml_element(frecord: FileRecord) -> ET.Element:
    file_el = ET.Element(f"{{{DAV_NS}}}response")
    href = ET.SubElement(file_el, f"{{{DAV_NS}}}href")
    href.text = f"/{frecord.url}"
    propstat = ET.SubElement(file_el, f"{{{DAV_NS}}}propstat")
    prop = ET.SubElement(propstat, f"{{{DAV_NS}}}prop")
    ET.SubElement(prop, f"{{{DAV_NS}}}displayname").text = decode_uri_components(frecord.url.split("/")[-1])
    ET.SubElement(prop, f"{{{DAV_NS}}}resourcetype")
    ET.SubElement(prop, f"{{{DAV_NS}}}getcontentlength").text = str(frecord.file_size)
    ET.SubElement(prop, f"{{{DAV_NS}}}getlastmodified").text = format_last_modified(frecord.create_time)
    ET.SubElement(prop, f"{{{DAV_NS}}}getcontenttype").text = frecord.mime_type
    lock_el = await query_lock_element(frecord.url, top_el_name=f"{{{DAV_NS}}}activelock")
    if lock_el is not None:
        lock_discovery = ET.SubElement(prop, f"{{{DAV_NS}}}lockdiscovery")
        lock_discovery.append(lock_el)
    ET.SubElement(propstat, f"{{{DAV_NS}}}status").text = "HTTP/1.1 200 OK"
    return file_el

async def create_dir_xml_element(drecord: DirectoryRecord) -> ET.Element:
    dir_el = ET.Element(f"{{{DAV_NS}}}response")
    href = ET.SubElement(dir_el, f"{{{DAV_NS}}}href")
    href.text = f"/{drecord.url}"
    propstat = ET.SubElement(dir_el, f"{{{DAV_NS}}}propstat")
    prop = ET.SubElement(propstat, f"{{{DAV_NS}}}prop")
    ET.SubElement(prop, f"{{{DAV_NS}}}displayname").text = decode_uri_components(drecord.url.split("/")[-2])
    ET.SubElement(prop, f"{{{DAV_NS}}}resourcetype").append(ET.Element(f"{{{DAV_NS}}}collection"))
    if drecord.size >= 0:
        ET.SubElement(prop, f"{{{DAV_NS}}}getlastmodified").text = format_last_modified(drecord.create_time)
        ET.SubElement(prop, f"{{{DAV_NS}}}getcontentlength").text = str(drecord.size)
    lock_el = await query_lock_element(drecord.url, top_el_name=f"{{{DAV_NS}}}activelock")
    if lock_el is not None:
        lock_discovery = ET.SubElement(prop, f"{{{DAV_NS}}}lockdiscovery")
        lock_discovery.append(lock_el)
    ET.SubElement(propstat, f"{{{DAV_NS}}}status").text = "HTTP/1.1 200 OK"
    return dir_el

async def xml_request_body(request: Request) -> Optional[ET.Element]:
    try:
        assert request.headers.get("Content-Type") == "application/xml"
        body = await request.body()
        return ET.fromstring(body)
    except Exception as e:
        return None

@router_dav.options("/{path:path}")
async def dav_options(request: Request, path: str):
    return Response(headers={
        "DAV": "1,2",
        "MS-Author-Via": "DAV",
        "Allow": "OPTIONS, GET, HEAD, POST, DELETE, TRACE, PROPFIND, PROPPATCH, COPY, MOVE, LOCK, UNLOCK, MKCOL",
        "Content-Length": "0"
    })

@router_dav.api_route("/{path:path}", methods=["PROPFIND"])
@handle_exception
async def dav_propfind(request: Request, path: str, user: UserRecord = Depends(registered_user), body: Optional[ET.Element] = Depends(xml_request_body)):
    if path.startswith("/"): path = path[1:]
    path = ensure_uri_components(path)

    if body and DEBUG_MODE:
        print("Propfind-body:", ET.tostring(body, encoding="utf-8", method="xml"))

    depth = request.headers.get("Depth", "0")
    # Generate XML response
    multistatus = ET.Element(f"{{{DAV_NS}}}multistatus")
    path_type, lfss_path, record = await eval_path(path)
    logger.info(f"PROPFIND {lfss_path} (depth: {depth}), type: {path_type}, record: {record}")

    if lfss_path and await check_path_permission(lfss_path, user) < AccessLevel.READ:
        raise PermissionDeniedError(lfss_path)

    if path_type == "dir" and depth == "0":
        # query the directory itself
        assert isinstance(record, DirectoryRecord)
        dir_el = await create_dir_xml_element(record)
        multistatus.append(dir_el)

    elif path_type == "dir" and lfss_path == "":
        # query root directory content
        async def user_path_record(user_name: str, cur) -> DirectoryRecord:
            try:
                return await FileConn(cur).get_dir_record(user_name + "/")
            except PathNotFoundError:
                return DirectoryRecord(user_name + "/", size=0, n_files=0, create_time="1970-01-01 00:00:00", update_time="1970-01-01 00:00:00", access_time="1970-01-01 00:00:00")

        async with unique_cursor() as c:
            uconn = UserConn(c)
            if not user.is_admin:
                for u in [user] + await uconn.list_peer_users(user.id, AccessLevel.READ):
                    dir_el = await create_dir_xml_element(await user_path_record(u.username, c))
                    multistatus.append(dir_el)
            else:
                async for u in uconn.iter_all():
                    dir_el = await create_dir_xml_element(await user_path_record(u.username, c))
                    multistatus.append(dir_el)

    elif path_type == "dir":
        # query directory content
        async with unique_cursor() as c:
            flist = await FileConn(c).list_dir_files(lfss_path, flat = True if depth == "infinity" else False)
        for frecord in flist:
            if frecord.url.endswith(f"/{MKDIR_PLACEHOLDER}"): continue
            file_el = await create_file_xml_element(frecord)
            multistatus.append(file_el)

        async with unique_cursor() as c:
            drecords = await FileConn(c).list_path_dirs(lfss_path)
        for drecord in drecords:
            dir_el = await create_dir_xml_element(drecord)
            multistatus.append(dir_el)

    elif path_type == "file": 
        # query file
        assert isinstance(record, FileRecord)
        file_el = await create_file_xml_element(record)
        multistatus.append(file_el)
    
    else:
        raise PathNotFoundError(path)

    xml_response = ET.tostring(multistatus, encoding="utf-8", method="xml")
    return Response(content=xml_response, media_type="application/xml", status_code=207)

@router_dav.api_route("/{path:path}", methods=["MKCOL"])
@handle_exception
async def dav_mkcol(path: str, user: UserRecord = Depends(registered_user)):
    # TODO: implement MKCOL more elegantly
    if path.endswith("/"): path = path[:-1]     # make sure returned path is a file
    ptype, lfss_path, _ = await eval_path(path)
    if not ptype is None:
        raise HTTPException(status_code=409, detail="Conflict")
    logger.info(f"MKCOL {path}")
    fpath = lfss_path + "/" + MKDIR_PLACEHOLDER
    async def _ustream():
        yield b""
    await db.save_file(user.username, fpath, _ustream())
    return Response(status_code=201)

@router_dav.api_route("/{path:path}", methods=["MOVE"])
@handle_exception
async def dav_move(request: Request, path: str, user: UserRecord = Depends(registered_user)):
    destination = request.headers.get("Destination")
    if not destination:
        raise HTTPException(status_code=400, detail="Destination header is required")

    ptype, lfss_path, _ = await eval_path(path)
    if ptype is None:
        raise PathNotFoundError(path)
    dptype, dlfss_path, _ = await eval_path(destination)
    if dptype is not None:
        raise HTTPException(status_code=409, detail="Conflict")

    logger.info(f"MOVE {path} -> {destination}")
    if ptype == "file":
        assert not lfss_path.endswith("/"), "File path should not end with /"
        assert not dlfss_path.endswith("/"), "File path should not end with /"
        await db.move_file(lfss_path, dlfss_path, user)
    else:
        assert ptype == "dir", "Directory path should end with /"
        assert lfss_path.endswith("/"), "Directory path should end with /"
        if not dlfss_path.endswith("/"): dlfss_path += "/"  # the header destination may not end with /
        await db.move_dir(lfss_path, dlfss_path, user)
    return Response(status_code=201)

@router_dav.api_route("/{path:path}", methods=["COPY"])
@handle_exception
async def dav_copy(request: Request, path: str, user: UserRecord = Depends(registered_user)):
    destination = request.headers.get("Destination")
    if not destination:
        raise HTTPException(status_code=400, detail="Destination header is required")

    ptype, lfss_path, _ = await eval_path(path)
    if ptype is None:
        raise PathNotFoundError(path)
    dptype, dlfss_path, _ = await eval_path(destination)
    if dptype is not None:
        raise HTTPException(status_code=409, detail="Conflict")
    
    logger.info(f"COPY {path} -> {destination}")
    return await copy_impl(op_user=user, src_path=lfss_path, dst_path=dlfss_path)

_dav_lock = asyncio.Lock()

@router_dav.api_route("/{path:path}", methods=["LOCK"])
@handle_exception
async def dav_lock(request: Request, path: str, user: UserRecord = Depends(registered_user), body: ET.Element = Depends(xml_request_body)):
    raw_timeout = request.headers.get("Timeout", "Second-3600")
    if raw_timeout == "Infinite": timeout = -1
    else:
        if not raw_timeout.startswith("Second-"): 
            raise HTTPException(status_code=400, detail="Bad Request, invalid timeout: " + raw_timeout + ", expected Second-<seconds> or Infinite")
        _, timeout_str = raw_timeout.split("-")
        timeout = int(timeout_str)
    
    lock_depth = request.headers.get("Depth", "0")
    _, path, _ = await eval_path(path)
    # lock_token = f"opaquelocktoken:{uuid.uuid4().hex}"
    lock_token = f"urn:uuid:{uuid.uuid4()}"
    logger.info(f"LOCK {path} (timeout: {timeout}), token: {lock_token}, depth: {lock_depth}")
    if DEBUG_MODE and body:
        print("Lock-body:", ET.tostring(body, encoding="utf-8", method="xml"))
    async with _dav_lock:
        await lock_path(user, path, lock_token, lock_depth, timeout=timeout)
        response_elem = ET.Element(f"{{{DAV_NS}}}prop")
        lockdiscovery = ET.SubElement(response_elem, f"{{{DAV_NS}}}lockdiscovery")
        activelock = await query_lock_element(path, top_el_name=f"{{{DAV_NS}}}activelock")
        assert activelock is not None
    lockdiscovery.append(activelock)
    lock_response = ET.tostring(response_elem, encoding="utf-8", method="xml")
    return Response(content=lock_response, media_type="application/xml", status_code=201, headers={
        "Lock-Token": f"<{lock_token}>"
    })

@router_dav.api_route("/{path:path}", methods=["UNLOCK"])
@handle_exception
async def dav_unlock(request: Request, path: str, user: UserRecord = Depends(registered_user), body: ET.Element = Depends(xml_request_body)):
    lock_token = request.headers.get("Lock-Token")
    if not lock_token:
        raise HTTPException(status_code=400, detail="Lock-Token header is required")
    if lock_token.startswith("<") and lock_token.endswith(">"):
        lock_token = lock_token[1:-1]
    logger.info(f"UNLOCK {path}, token: {lock_token}")
    if DEBUG_MODE and body:
        print("Unlock-body:", ET.tostring(body, encoding="utf-8", method="xml"))
    _, path, _ = await eval_path(path)
    await unlock_path(user, path, lock_token)
    return Response(status_code=204)

@router_dav.api_route("/{path:path}", methods=["PROPPATCH"])
@handle_exception
async def dav_proppatch(request: Request, path: str, user: UserRecord = Depends(registered_user), body: ET.Element = Depends(xml_request_body)):
    # TODO: implement PROPPATCH
    logger.debug(f"PROPPATCH {path} (not implemented)")
    raise HTTPException(status_code=501, detail="PROPPATCH is not implemented")

__all__ = ["router_dav"]