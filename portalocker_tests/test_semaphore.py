import random

import pytest

import portalocker
from portalocker import utils


@pytest.mark.parametrize('timeout', [None, 0, 0.001])
@pytest.mark.parametrize('check_interval', [None, 0, 0.0005])
def test_bounded_semaphore(timeout, check_interval, monkeypatch):
    n = 2
    name: str = str(random.random())
    monkeypatch.setattr(utils, 'DEFAULT_TIMEOUT', 0.0001)
    monkeypatch.setattr(utils, 'DEFAULT_CHECK_INTERVAL', 0.0005)

    semaphore_a = portalocker.BoundedSemaphore(n, name=name, timeout=timeout)
    semaphore_b = portalocker.BoundedSemaphore(n, name=name, timeout=timeout)
    semaphore_c = portalocker.BoundedSemaphore(n, name=name, timeout=timeout)

    # First acquire should succeed
    semaphore_a.acquire(timeout=timeout)

    # Second acquire should succeed
    semaphore_b.acquire()

    # Third acquire should fail with AlreadyLocked
    with pytest.raises(portalocker.AlreadyLocked):
        semaphore_c.acquire(check_interval=check_interval, timeout=timeout)

    # Release one semaphore
    semaphore_a.release()

    # Now the third acquire should succeed
    semaphore_c.acquire(
        check_interval=check_interval,
        timeout=timeout,
        fail_when_locked=False,
    )
