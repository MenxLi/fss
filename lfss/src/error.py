
class LFSSExceptionBase(Exception):...

class PermissionDeniedError(LFSSExceptionBase, PermissionError):...

class StorageExceededError(LFSSExceptionBase):...