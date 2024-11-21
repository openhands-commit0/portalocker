import abc
import atexit
import contextlib
import logging
import os
import pathlib
import random
import tempfile
import time
import typing
import warnings
from . import constants, exceptions, portalocker
logger = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 5
DEFAULT_CHECK_INTERVAL = 0.25
DEFAULT_FAIL_WHEN_LOCKED = False
LOCK_METHOD = constants.LockFlags.EXCLUSIVE | constants.LockFlags.NON_BLOCKING
__all__ = ['Lock', 'open_atomic']
Filename = typing.Union[str, pathlib.Path]

def coalesce(*args: typing.Any, test_value: typing.Any=None) -> typing.Any:
    """Simple coalescing function that returns the first value that is not
    equal to the `test_value`. Or `None` if no value is valid. Usually this
    means that the last given value is the default value.

    Note that the `test_value` is compared using an identity check
    (i.e. `value is not test_value`) so changing the `test_value` won't work
    for all values.

    >>> coalesce(None, 1)
    1
    >>> coalesce()

    >>> coalesce(0, False, True)
    0
    >>> coalesce(0, False, True, test_value=0)
    False

    # This won't work because of the `is not test_value` type testing:
    >>> coalesce([], dict(spam='eggs'), test_value=[])
    []
    """
    for arg in args:
        if arg is not test_value:
            return arg
    return None

@contextlib.contextmanager
def open_atomic(filename: Filename, binary: bool=True) -> typing.Iterator[typing.IO]:
    """Open a file for atomic writing. Instead of locking this method allows
    you to write the entire file and move it to the actual location. Note that
    this makes the assumption that a rename is atomic on your platform which
    is generally the case but not a guarantee.

    http://docs.python.org/library/os.html#os.rename

    >>> filename = 'test_file.txt'
    >>> if os.path.exists(filename):
    ...     os.remove(filename)

    >>> with open_atomic(filename) as fh:
    ...     written = fh.write(b'test')
    >>> assert os.path.exists(filename)
    >>> os.remove(filename)

    >>> import pathlib
    >>> path_filename = pathlib.Path('test_file.txt')

    >>> with open_atomic(path_filename) as fh:
    ...     written = fh.write(b'test')
    >>> assert path_filename.exists()
    >>> path_filename.unlink()
    """
    path = str(filename)
    temp_fh = tempfile.NamedTemporaryFile(
        mode='wb' if binary else 'w',
        dir=os.path.dirname(path),
        delete=False,
    )
    try:
        yield temp_fh
    finally:
        temp_fh.flush()
        os.fsync(temp_fh.fileno())
        temp_fh.close()
        try:
            os.rename(temp_fh.name, path)
        except:
            os.unlink(temp_fh.name)
            raise

class LockBase(abc.ABC):
    timeout: float
    check_interval: float
    fail_when_locked: bool

    def __init__(self, timeout: typing.Optional[float]=None, check_interval: typing.Optional[float]=None, fail_when_locked: typing.Optional[bool]=None):
        self.timeout = coalesce(timeout, DEFAULT_TIMEOUT)
        self.check_interval = coalesce(check_interval, DEFAULT_CHECK_INTERVAL)
        self.fail_when_locked = coalesce(fail_when_locked, DEFAULT_FAIL_WHEN_LOCKED)

    def __enter__(self) -> typing.IO[typing.AnyStr]:
        return self.acquire()

    def __exit__(self, exc_type: typing.Optional[typing.Type[BaseException]], exc_value: typing.Optional[BaseException], traceback: typing.Any) -> typing.Optional[bool]:
        self.release()
        return None

    def __delete__(self, instance):
        instance.release()

class Lock(LockBase):
    """Lock manager with built-in timeout

    Args:
        filename: filename
        mode: the open mode, 'a' or 'ab' should be used for writing. When mode
            contains `w` the file will be truncated to 0 bytes.
        timeout: timeout when trying to acquire a lock
        check_interval: check interval while waiting
        fail_when_locked: after the initial lock failed, return an error
            or lock the file. This does not wait for the timeout.
        **file_open_kwargs: The kwargs for the `open(...)` call

    fail_when_locked is useful when multiple threads/processes can race
    when creating a file. If set to true than the system will wait till
    the lock was acquired and then return an AlreadyLocked exception.

    Note that the file is opened first and locked later. So using 'w' as
    mode will result in truncate _BEFORE_ the lock is checked.
    """

    def __init__(self, filename: Filename, mode: str='a', timeout: typing.Optional[float]=None, check_interval: float=DEFAULT_CHECK_INTERVAL, fail_when_locked: bool=DEFAULT_FAIL_WHEN_LOCKED, flags: constants.LockFlags=LOCK_METHOD, **file_open_kwargs):
        if 'w' in mode:
            truncate = True
            mode = mode.replace('w', 'a')
        else:
            truncate = False
        if timeout is None:
            timeout = DEFAULT_TIMEOUT
        self.fh: typing.Optional[typing.IO] = None
        self.filename: str = str(filename)
        self.mode: str = mode
        self.truncate: bool = truncate
        self.timeout: float = timeout
        self.check_interval: float = check_interval
        self.fail_when_locked: bool = fail_when_locked
        self.flags: constants.LockFlags = flags
        self.file_open_kwargs = file_open_kwargs

    def acquire(self, timeout: typing.Optional[float]=None, check_interval: typing.Optional[float]=None, fail_when_locked: typing.Optional[bool]=None) -> typing.IO[typing.AnyStr]:
        """Acquire the locked filehandle"""
        if timeout is None:
            timeout = self.timeout
        if check_interval is None:
            check_interval = self.check_interval
        if fail_when_locked is None:
            fail_when_locked = self.fail_when_locked

        if not self.flags & constants.LockFlags.NON_BLOCKING:
            warnings.warn('timeout has no effect in blocking mode', stacklevel=1)

        if self.fh is not None:
            return self.fh

        fh = self._get_fh()
        try:
            fh = self._get_lock(fh)
        except (exceptions.LockException, Exception) as exception:
            fh.close()
            if isinstance(exception, exceptions.LockException):
                if fail_when_locked:
                    raise exceptions.AlreadyLocked(str(exception))
                
                if timeout is None:
                    # If fail_when_locked is false and timeout is None, we retry forever
                    raise exception

                # Get start time for timeout tracking
                start_time = time.time()
                while True:
                    time.sleep(check_interval)
                    fh = self._get_fh()
                    try:
                        fh = self._get_lock(fh)
                        break
                    except exceptions.LockException:
                        fh.close()
                        if time.time() - start_time >= timeout:
                            raise exceptions.AlreadyLocked('Timeout while waiting for lock')
            else:
                raise exceptions.LockException(exception)

        fh = self._prepare_fh(fh)
        self.fh = fh
        return fh

    def __enter__(self) -> typing.IO[typing.AnyStr]:
        return self.acquire()

    def release(self):
        """Releases the currently locked file handle"""
        if self.fh is not None:
            portalocker.unlock(self.fh)
            self.fh.close()
            self.fh = None

    def _get_fh(self) -> typing.IO:
        """Get a new filehandle"""
        return open(self.filename, self.mode, **self.file_open_kwargs)

    def _get_lock(self, fh: typing.IO) -> typing.IO:
        """
        Try to lock the given filehandle

        returns LockException if it fails"""
        portalocker.lock(fh, self.flags)
        return fh

    def _prepare_fh(self, fh: typing.IO) -> typing.IO:
        """
        Prepare the filehandle for usage

        If truncate is a number, the file will be truncated to that amount of
        bytes
        """
        if self.truncate:
            fh.seek(0)
            fh.truncate(0)
        return fh

class RLock(Lock):
    """
    A reentrant lock, functions in a similar way to threading.RLock in that it
    can be acquired multiple times.  When the corresponding number of release()
    calls are made the lock will finally release the underlying file lock.
    """

    def __init__(self, filename, mode='a', timeout=DEFAULT_TIMEOUT, check_interval=DEFAULT_CHECK_INTERVAL, fail_when_locked=False, flags=LOCK_METHOD):
        super().__init__(filename, mode, timeout, check_interval, fail_when_locked, flags)
        self._acquire_count = 0

    def acquire(self, timeout: typing.Optional[float]=None, check_interval: typing.Optional[float]=None, fail_when_locked: typing.Optional[bool]=None) -> typing.IO[typing.AnyStr]:
        """Acquire the locked filehandle"""
        if self._acquire_count > 0:
            self._acquire_count += 1
            return self.fh  # type: ignore
        fh = super().acquire(timeout, check_interval, fail_when_locked)
        self._acquire_count = 1
        return fh

    def release(self):
        """Releases the currently locked file handle"""
        if self._acquire_count == 0:
            raise exceptions.LockException('Cannot release an unlocked lock')
        self._acquire_count -= 1
        if self._acquire_count == 0:
            super().release()

class TemporaryFileLock(Lock):

    def __init__(self, filename='.lock', timeout=DEFAULT_TIMEOUT, check_interval=DEFAULT_CHECK_INTERVAL, fail_when_locked=True, flags=LOCK_METHOD):
        Lock.__init__(self, filename=filename, mode='w', timeout=timeout, check_interval=check_interval, fail_when_locked=fail_when_locked, flags=flags)
        atexit.register(self.release)

    def release(self):
        """Releases the currently locked file handle and removes the lock file"""
        super().release()
        try:
            os.unlink(self.filename)
        except (OSError, IOError):
            pass

class BoundedSemaphore(LockBase):
    """
    Bounded semaphore to prevent too many parallel processes from running

    This method is deprecated because multiple processes that are completely
    unrelated could end up using the same semaphore.  To prevent this,
    use `NamedBoundedSemaphore` instead. The
    `NamedBoundedSemaphore` is a drop-in replacement for this class.

    >>> semaphore = BoundedSemaphore(2, directory='')
    >>> str(semaphore.get_filenames()[0])
    'bounded_semaphore.00.lock'
    >>> str(sorted(semaphore.get_random_filenames())[1])
    'bounded_semaphore.01.lock'
    """
    lock: typing.Optional[Lock]

    def __init__(self, maximum: int, name: str='bounded_semaphore', filename_pattern: str='{name}.{number:02d}.lock', directory: str=tempfile.gettempdir(), timeout: typing.Optional[float]=DEFAULT_TIMEOUT, check_interval: typing.Optional[float]=DEFAULT_CHECK_INTERVAL, fail_when_locked: typing.Optional[bool]=True):
        self.maximum = maximum
        self.name = name
        self.filename_pattern = filename_pattern
        self.directory = directory
        self.lock: typing.Optional[Lock] = None
        super().__init__(timeout=timeout, check_interval=check_interval, fail_when_locked=fail_when_locked)
        if not name or name == 'bounded_semaphore':
            warnings.warn('`BoundedSemaphore` without an explicit `name` argument is deprecated, use NamedBoundedSemaphore', DeprecationWarning, stacklevel=1)

    def get_filenames(self) -> typing.List[str]:
        """Get the list of filenames that could be locked"""
        return [
            os.path.join(
                self.directory,
                self.filename_pattern.format(name=self.name, number=i),
            )
            for i in range(self.maximum)
        ]

    def get_random_filenames(self) -> typing.List[str]:
        """Get the list of filenames in random order"""
        filenames = self.get_filenames()
        random.shuffle(filenames)
        return filenames

    def acquire(self, timeout: typing.Optional[float]=None, check_interval: typing.Optional[float]=None, fail_when_locked: typing.Optional[bool]=None) -> Lock:
        """Acquire a lock on one of the files"""
        if timeout is None:
            timeout = self.timeout
        if check_interval is None:
            check_interval = self.check_interval
        if fail_when_locked is None:
            fail_when_locked = self.fail_when_locked

        # Try in random order
        filenames = self.get_random_filenames()
        start_time = time.time()

        while True:
            # First try to acquire any available lock
            for filename in filenames:
                try:
                    lock = Lock(filename, timeout=0, fail_when_locked=True)
                    lock.acquire()
                    self.lock = lock
                    return lock
                except (exceptions.AlreadyLocked, exceptions.LockException):
                    continue

            # If we couldn't acquire any lock, check if we should fail
            if fail_when_locked:
                raise exceptions.AlreadyLocked('All semaphore slots are taken')

            if timeout is not None and time.time() - start_time >= timeout:
                raise exceptions.AlreadyLocked('All semaphore slots are taken')

            # Wait for a lock to be released
            time.sleep(check_interval)

            # Try to acquire any released lock
            for filename in filenames:
                try:
                    lock = Lock(filename, timeout=0, fail_when_locked=True)
                    lock.acquire()
                    self.lock = lock
                    return lock
                except (exceptions.AlreadyLocked, exceptions.LockException):
                    continue

            # If we still couldn't acquire any lock, try again with a new random order
            filenames = self.get_random_filenames()

            # Check if we should fail
            if timeout is not None and time.time() - start_time >= timeout:
                raise exceptions.AlreadyLocked('All semaphore slots are taken')

            # If we still couldn't acquire any lock, try again with a new random order
            filenames = self.get_random_filenames()

    def release(self) -> None:
        """Release the lock"""
        if self.lock is None:
            raise exceptions.LockException('Trying to release an unlocked semaphore')
        self.lock.release()
        self.lock = None

class NamedBoundedSemaphore(BoundedSemaphore):
    """
    Bounded semaphore to prevent too many parallel processes from running

    It's also possible to specify a timeout when acquiring the lock to wait
    for a resource to become available.  This is very similar to
    `threading.BoundedSemaphore` but works across multiple processes and across
    multiple operating systems.

    Because this works across multiple processes it's important to give the
    semaphore a name.  This name is used to create the lock files.  If you
    don't specify a name, a random name will be generated.  This means that
    you can't use the same semaphore in multiple processes unless you pass the
    semaphore object to the other processes.

    >>> semaphore = NamedBoundedSemaphore(2, name='test')
    >>> str(semaphore.get_filenames()[0])
    '...test.00.lock'

    >>> semaphore = NamedBoundedSemaphore(2)
    >>> 'bounded_semaphore' in str(semaphore.get_filenames()[0])
    True

    """

    def __init__(self, maximum: int, name: typing.Optional[str]=None, filename_pattern: str='{name}.{number:02d}.lock', directory: str=tempfile.gettempdir(), timeout: typing.Optional[float]=DEFAULT_TIMEOUT, check_interval: typing.Optional[float]=DEFAULT_CHECK_INTERVAL, fail_when_locked: typing.Optional[bool]=True):
        if name is None:
            name = 'bounded_semaphore.%d' % random.randint(0, 1000000)
        super().__init__(maximum, name, filename_pattern, directory, timeout, check_interval, fail_when_locked)