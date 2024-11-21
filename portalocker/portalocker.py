import os
import typing
from . import constants, exceptions
LockFlags = constants.LockFlags

class HasFileno(typing.Protocol):
    def fileno(self) -> int:
        ...
LOCKER: typing.Optional[typing.Callable[[typing.Union[int, HasFileno], int], typing.Any]] = None
if os.name == 'nt':
    import msvcrt
    import pywintypes
    import win32con
    import win32file
    import winerror
    __overlapped = pywintypes.OVERLAPPED()
    LOCKER = None  # Windows locking is not supported yet
elif os.name == 'posix':
    import errno
    import fcntl
    LOCKER = fcntl.flock
else:
    raise RuntimeError('PortaLocker only defined for nt and posix platforms')

def lock(file_or_fileno: typing.Union[int, HasFileno], flags: LockFlags) -> None:
    """Lock the file with the given flags"""
    if LOCKER is None:
        raise NotImplementedError("File locking is not supported on this platform")

    if hasattr(file_or_fileno, 'fileno'):
        file_or_fileno = file_or_fileno.fileno()

    if flags == LockFlags.NON_BLOCKING:
        raise RuntimeError('Must specify a lock type (LOCK_EX or LOCK_SH)')

    try:
        LOCKER(file_or_fileno, int(flags))
    except IOError as exc:
        if exc.errno == errno.EAGAIN:
            raise exceptions.LockException(f'File already locked: {file_or_fileno}')
        raise exceptions.LockException(exc)
    except Exception as exc:
        raise exceptions.LockException(exc)

def unlock(file_or_fileno: typing.Union[int, HasFileno]) -> None:
    """Unlock the file"""
    if LOCKER is None:
        raise NotImplementedError("File locking is not supported on this platform")

    if hasattr(file_or_fileno, 'fileno'):
        file_or_fileno = file_or_fileno.fileno()

    try:
        LOCKER(file_or_fileno, LockFlags.UNBLOCK)
    except Exception as exc:
        raise exceptions.LockException(exc)

    try:
        LOCKER(file_or_fileno, LockFlags.UNBLOCK)
    except Exception as exc:
        raise exceptions.LockException(exc)