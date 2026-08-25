import asyncio, time, os
from contextlib import asynccontextmanager
from typing import Optional
from functools import wraps

from fastapi import FastAPI, HTTPException, Request, Response, APIRouter, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, HTTPBasic, HTTPBasicCredentials

from ..eng.log import get_logger
from ..eng.datatype import UserRecord, HttpRecord
from ..eng.datatype import DECOY_USER
from ..eng.database_conn import delayed_log_activity
from ..eng.userman import UserCtl
from ..eng.database import Database
from ..eng.connection_pool import global_connection_init, global_connection_close
from ..eng.utils import wait_for_debounce_tasks, now_stamp, hash_credential
from ..eng.error import *
from ..eng.config import DEBUG_MODE
from ..eng.request_log import RequestDB

ENABLE_WEBDAV = os.environ.get("LFSS_WEBDAV", "0") == "1"
logger = get_logger("server")
logger_failed_request = get_logger("failed_requests", term_level="INFO")
db = Database()
req_conn = RequestDB()


class ByteCountMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_size_actual = 0
        response_size_actual = 0
        state = scope.setdefault("state", {})

        async def counted_receive():
            nonlocal request_size_actual
            message = await receive()
            if message["type"] == "http.request":
                request_size_actual += len(message.get("body", b""))
                state["request_size_actual"] = request_size_actual
            return message

        async def counted_send(message):
            nonlocal response_size_actual
            if message["type"] == "http.response.body":
                response_size_actual += len(message.get("body", b""))
                state["response_size_actual"] = response_size_actual
            await send(message)

        await self.app(scope, counted_receive, counted_send)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db
    try:
        await global_connection_init(n_read = 8 if not DEBUG_MODE else 1)
        await asyncio.gather(db.init(), req_conn.init())
        yield
        await req_conn.flush()
    finally:
        await wait_for_debounce_tasks()
        await global_connection_close()

def handle_exception(fn):
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        try:
            return await fn(*args, **kwargs)
        except Exception as e:
            if isinstance(e, HTTPException): 
                print(f"HTTPException: {e}, detail: {e.detail}")
            if isinstance(e, HTTPException): raise e
            if isinstance(e, StorageExceededError): raise HTTPException(status_code=413, detail=str(e))
            if isinstance(e, PermissionError): raise HTTPException(status_code=403, detail=str(e))
            if isinstance(e, InvalidPathError): raise HTTPException(status_code=400, detail=str(e))
            if isinstance(e, InvalidOptionsError): raise HTTPException(status_code=400, detail=str(e))
            if isinstance(e, InvalidDataError): raise HTTPException(status_code=400, detail=str(e))
            if isinstance(e, FileNotFoundError): raise HTTPException(status_code=404, detail=str(e))
            if isinstance(e, FileDuplicateError): raise HTTPException(status_code=409, detail=str(e))
            if isinstance(e, FileExistsError): raise HTTPException(status_code=409, detail=str(e))
            if isinstance(e, UserNotFoundError): raise HTTPException(status_code=404, detail=str(e))
            if isinstance(e, TooManyItemsError): raise HTTPException(status_code=400, detail=str(e))
            if isinstance(e, DatabaseLockedError): raise HTTPException(status_code=503, detail=str(e))
            if isinstance(e, DatabaseTransactionError): raise HTTPException(status_code=503, detail=str(e))
            if isinstance(e, FileLockedError): raise HTTPException(status_code=423, detail=str(e))
            logger.error(f"Uncaptured error in {fn.__name__}: {e}")
            raise 
    return wrapper

env_origins = os.environ.get("LFSS_ORIGIN", "*")
logger.debug(f"LFSS_ORIGIN: {env_origins}")
origins = [x.strip() for x in env_origins.split(",") if x.strip()]
app = FastAPI(docs_url=None, redoc_url=None, lifespan=lifespan)
app.add_middleware(ByteCountMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=False,   # auth is header/query based, no cookie credentials needed
    allow_methods=["*"],
    allow_headers=["*"],
)


def _safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _mask_auth_headers(headers: dict) -> str:
    """ Mask sensitive headers before persisting them to the request log. """
    masked = {
        k: ("Bearer ***" if k.lower() == "authorization" and v else v)
        for k, v in headers.items()
    }
    return str(masked)

def _mask_query(query_params) -> str:
    """ Mask credential query params before persisting them to the request log. """
    masked = {
        k: ("***" if k.lower() == "token" else v)
        for k, v in dict(query_params).items()
    }
    return str(masked)

@app.middleware("http")
async def log_requests(request: Request, call_next):

    request_time_stamp = now_stamp()
    start_time = time.perf_counter()
    response: Response = await call_next(request)
    end_time = time.perf_counter()
    response_time = end_time - start_time
    response.headers["X-Response-Time"] = str(response_time)

    if response.headers.get("X-Skip-Log", None) is not None:
        return response

    if response.status_code >= 400:
        logger_failed_request.error(f"{request.method} {request.url.path} \033[91m{response.status_code}\033[0m")
    if DEBUG_MODE:
        print(f"{request.method} {request.url.path} {response.status_code} {response_time:.3f}s")
        print(f"Request headers: {dict(request.headers)}")
    await req_conn.log_request(
        request_time_stamp, 
        HttpRecord(
            method = request.method, 
            path = request.url.path, 
            status = response.status_code, 
            duration = response_time,
            headers = _mask_auth_headers(dict(request.headers)), 
            query = _mask_query(dict(request.query_params)), 
            client = str(request.client),
            request_size = getattr(request.state, "request_size_actual", _safe_int(request.headers.get("Content-Length", 0))),
            response_size = getattr(request.state, "response_size_actual", _safe_int(response.headers.get("Content-Length", 0)))
        )
    )
    await req_conn.ensure_flush_once()
    return response

def skip_request_log(fn):
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        response = await fn(*args, **kwargs)
        assert isinstance(response, Response), "Response expected"
        response.headers["X-Skip-Log"] = "1"
        return response
    return wrapper

async def get_credential_from_params(request: Request):
    return request.query_params.get("token")
async def get_current_user(
    h_token: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False)), 
    b_token: Optional[HTTPBasicCredentials] = Depends(HTTPBasic(auto_error=False)),
    q_token: Optional[str] = Depends(get_credential_from_params)
    ):
    """
    First try to get the user from the bearer token, 
    if not found, try to get the user from the query parameter
    """
    if h_token:
        user = await UserCtl.get_user_by_credential(h_token.credentials, check_expire=True)
        if not user: raise HTTPException(status_code=401, detail="Invalid token", headers={"WWW-Authenticate": "Basic" if ENABLE_WEBDAV else "Bearer"})
    elif ENABLE_WEBDAV and b_token:
        user = await UserCtl.get_user_by_credential(hash_credential(b_token.username, b_token.password), check_expire=True)
        if not user: raise HTTPException(status_code=401, detail="Invalid token", headers={"WWW-Authenticate": "Basic" if ENABLE_WEBDAV else "Bearer"})
    elif q_token:
        user = await UserCtl.get_user_by_credential(q_token, check_expire=True)
        if not user: raise HTTPException(status_code=401, detail="Invalid token", headers={"WWW-Authenticate": "Basic" if ENABLE_WEBDAV else "Bearer"})
    else:
        return DECOY_USER

    if not user.id == 0:
        await delayed_log_activity(user.username)

    return user

async def registered_user(user: UserRecord = Depends(get_current_user)):
    if user.id == 0:
        raise HTTPException(status_code=401, detail="Permission denied", headers={"WWW-Authenticate": "Basic" if ENABLE_WEBDAV else "Bearer"})
    return user
async def admin_user(user: UserRecord = Depends(get_current_user)):
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin permission required")
    return user

router_api = APIRouter()
router_user = APIRouter()
router_dav = APIRouter()
router_fs = APIRouter()
router_metric = APIRouter()

__all__ = [
    "app", "db", "logger", 
    "handle_exception", "skip_request_log", 
    "get_current_user", "registered_user", "admin_user",
    "router_api", 
    "router_user", 
    "router_fs", 
    "router_dav", 
    "router_metric",
    ]