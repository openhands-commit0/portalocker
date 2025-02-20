import _thread
import json
import logging
import random
import time
import typing
from redis import client
from . import exceptions, utils
logger = logging.getLogger(__name__)
DEFAULT_UNAVAILABLE_TIMEOUT = 1
DEFAULT_THREAD_SLEEP_TIME = 0.1

class PubSubWorkerThread(client.PubSubWorkerThread):
    pass

class RedisLock(utils.LockBase):
    """
    An extremely reliable Redis lock based on pubsub with a keep-alive thread

    As opposed to most Redis locking systems based on key/value pairs,
    this locking method is based on the pubsub system. The big advantage is
    that if the connection gets killed due to network issues, crashing
    processes or otherwise, it will still immediately unlock instead of
    waiting for a lock timeout.

    To make sure both sides of the lock know about the connection state it is
    recommended to set the `health_check_interval` when creating the redis
    connection..

    Args:
        channel: the redis channel to use as locking key.
        connection: an optional redis connection if you already have one
        or if you need to specify the redis connection
        timeout: timeout when trying to acquire a lock
        check_interval: check interval while waiting
        fail_when_locked: after the initial lock failed, return an error
            or lock the file. This does not wait for the timeout.
        thread_sleep_time: sleep time between fetching messages from redis to
            prevent a busy/wait loop. In the case of lock conflicts this
            increases the time it takes to resolve the conflict. This should
            be smaller than the `check_interval` to be useful.
        unavailable_timeout: If the conflicting lock is properly connected
            this should never exceed twice your redis latency. Note that this
            will increase the wait time possibly beyond your `timeout` and is
            always executed if a conflict arises.
        redis_kwargs: The redis connection arguments if no connection is
            given. The `DEFAULT_REDIS_KWARGS` are used as default, if you want
            to override these you need to explicitly specify a value (e.g.
            `health_check_interval=0`)

    """
    redis_kwargs: typing.Dict[str, typing.Any]
    thread: typing.Optional[PubSubWorkerThread]
    channel: str
    timeout: float
    connection: typing.Optional[client.Redis]
    pubsub: typing.Optional[client.PubSub] = None
    close_connection: bool
    DEFAULT_REDIS_KWARGS: typing.ClassVar[typing.Dict[str, typing.Any]] = dict(health_check_interval=10)

    def __init__(self, channel: str, connection: typing.Optional[client.Redis]=None, timeout: typing.Optional[float]=None, check_interval: typing.Optional[float]=None, fail_when_locked: typing.Optional[bool]=False, thread_sleep_time: float=DEFAULT_THREAD_SLEEP_TIME, unavailable_timeout: float=DEFAULT_UNAVAILABLE_TIMEOUT, redis_kwargs: typing.Optional[typing.Dict]=None):
        self.close_connection = not connection
        self.thread = None
        self.channel = channel
        self.connection = connection
        self.thread_sleep_time = thread_sleep_time
        self.unavailable_timeout = unavailable_timeout
        self.redis_kwargs = redis_kwargs or dict()
        for key, value in self.DEFAULT_REDIS_KWARGS.items():
            self.redis_kwargs.setdefault(key, value)
        super().__init__(timeout=timeout, check_interval=check_interval, fail_when_locked=fail_when_locked)

    def __del__(self):
        self.release()