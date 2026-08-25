from .config import LOG_DIR, DISABLE_LOGGING, DEBUG_MODE
import time, sqlite3, dataclasses
from typing import TypeVar, Callable, Literal, Optional
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import logging, asyncio
from logging import handlers

class BCOLORS:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    OKGRAY = '\033[90m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    # Additional colors
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    LIGHTGRAY = '\033[37m'
    DARKGRAY = '\033[90m'
    LIGHTRED = '\033[91m'
    LIGHTGREEN = '\033[92m'
    LIGHTYELLOW = '\033[93m'
    LIGHTBLUE = '\033[94m'
    LIGHTMAGENTA = '\033[95m'
    LIGHTCYAN = '\033[96m'

_thread_pool = ThreadPoolExecutor(max_workers=1)
def thread_wrap(func):
    def wrapper(*args, **kwargs):
        _thread_pool.submit(func, *args, **kwargs)
    return wrapper

class BaseLogger(logging.Logger):
    def finalize(self):
        for handler in self.handlers:
            handler.flush()
            handler.close()
            self.removeHandler(handler)
    
    @thread_wrap
    def debug(self, *args, **kwargs): super().debug(*args, **kwargs)
    @thread_wrap
    def info(self, *args, **kwargs): super().info(*args, **kwargs)
    @thread_wrap
    def warning(self, *args, **kwargs): super().warning(*args, **kwargs)
    @thread_wrap
    def error(self, *args, **kwargs): super().error(*args, **kwargs)

class SQLiteFileHandler(logging.FileHandler):
    def __init__(self, filename, *args, **kwargs):
        super().__init__(filename, *args, **kwargs)
        self._db_file = filename
        self._buffer: list[logging.LogRecord] = []
        self._buffer_size = 100
        self._flush_interval = 10
        self._last_flush = time.time()
        conn = sqlite3.connect(self._db_file, check_same_thread=False)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created TIMESTAMP,
                created_epoch FLOAT,
                name TEXT,
                levelname VARCHAR(16),
                level INTEGER,
                message TEXT
            )
        ''')
        conn.commit()
        conn.close()
    
    def flush(self): 
        def format_time(self, record: logging.LogRecord):
            """ Create a time stamp """
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created))
        self.acquire()
        try:
            try:
                conn = sqlite3.connect(self._db_file, check_same_thread=False)
            except sqlite3.OperationalError:
                # the log dir/file may have been removed (e.g. after shutdown),
                # drop the buffer instead of raising (esp. at atexit)
                self._buffer.clear()
                return
            conn.executemany('''
                INSERT INTO log (created, created_epoch, name, levelname, level, message)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', [
                (format_time(self, record), record.created, record.name, record.levelname, record.levelno, record.getMessage())
                for record in self._buffer
            ])
            conn.commit()
            conn.close()
            self._buffer.clear()
            self._last_flush = time.time()
        finally:
            self.release()

    def emit(self, record: logging.LogRecord):
        self._buffer.append(record)
        if len(self._buffer) > self._buffer_size or time.time() - self._last_flush > self._flush_interval:
            self.flush()
    
    def close(self):
        self.flush()
        return super().close()

def eval_logline(row: sqlite3.Row):
    @dataclasses.dataclass
    class DBLogRecord:
        id: int
        created: str
        created_epoch: float
        name: str
        levelname: str
        level: int
        message: str
    return DBLogRecord(*row)

_fh_T = Literal['rotate', 'simple', 'daily', 'sqlite']

__g_logger_dict: dict[str, BaseLogger] = {}
def get_logger(
    name = 'default', 
    log_home = LOG_DIR, 
    level = 'DEBUG',
    term_level = 'INFO' if not DEBUG_MODE else 'DEBUG',
    file_handler_type: _fh_T = 'sqlite', 
    global_instance = True
    )->BaseLogger:
    if global_instance and name in __g_logger_dict:
        return __g_logger_dict[name]

    def setupLogger(logger: BaseLogger):
        logger.setLevel(level)

        format_str = BCOLORS.LIGHTMAGENTA + ' %(asctime)s ' +BCOLORS.OKCYAN + '[%(name)s][%(levelname)s] ' + BCOLORS.ENDC + ' %(message)s'
        formatter = logging.Formatter(format_str)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(term_level)
        logger.addHandler(console_handler)

        # format_str_plain = format_str.replace(BCOLORS.LIGHTMAGENTA, '').replace(BCOLORS.OKCYAN, '').replace(BCOLORS.ENDC, '')
        format_str_plain = format_str
        for color in BCOLORS.__dict__.values():
            if isinstance(color, str) and color.startswith('\033'):
                format_str_plain = format_str_plain.replace(color, '')

        if not DISABLE_LOGGING:
            formatter_plain = logging.Formatter(format_str_plain)
            log_home.mkdir(exist_ok=True)
            log_file = log_home / f'{name}.log'
            if file_handler_type == 'simple':
                file_handler = logging.FileHandler(log_file)
            elif file_handler_type == 'daily':
                file_handler = handlers.TimedRotatingFileHandler(
                    log_file, when='midnight', interval=1, backupCount=30
                )
            elif file_handler_type == 'rotate':
                file_handler = handlers.RotatingFileHandler(
                    log_file, maxBytes=1024*1024, backupCount=5
                )
            elif file_handler_type == 'sqlite':
                file_handler = SQLiteFileHandler(log_file if log_file.suffix == '.db' else log_file.with_suffix('.log.db'))

            file_handler.setFormatter(formatter_plain)
            logger.addHandler(file_handler)
    
    logger = BaseLogger(name)
    setupLogger(logger)
    if global_instance:
        __g_logger_dict[name] = logger

    return logger

def clear_handlers(logger: logging.Logger):
    for handler in logger.handlers:
        handler.flush()
        handler.close()
        logger.removeHandler(handler)
    __g_logger_dict.pop(logger.name, None)
    # print(f'Cleared handlers for logger {logger.name}')

FUNCTION_T = TypeVar('FUNCTION_T', bound=Callable)
def log_access(
    include_args: bool = True,
    logger: Optional[BaseLogger] = None, 
):
    if logger is None:
        logger = get_logger()

    def _log_access(fn: FUNCTION_T) -> FUNCTION_T:
        if asyncio.iscoroutinefunction(fn):
            @wraps(fn)
            async def async_wrapper(*args, **kwargs):
                if include_args:
                    logger.info(f'[func] <{fn.__name__}> called with: {args}, {kwargs}')
                else:
                    logger.info(f'[func] <{fn.__name__}>')
                    
                return await fn(*args, **kwargs)
            return async_wrapper    # type: ignore
        else:
            @wraps(fn)
            def wrapper(*args, **kwargs):
                logger = get_logger()
                if include_args:
                    logger.info(f'[func] <{fn.__name__}> called with: {args}, {kwargs}')
                else:
                    logger.info(f'[func] <{fn.__name__}>')
                    
                return fn(*args, **kwargs)
            return wrapper          # type: ignore
    return _log_access

__ALL__ = [
    'get_logger', 'log_access'
]