import asyncio
import logging
from typing import Optional, Self

from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from backend.env_loader import EnvLoader

FieldT = str | int | float | bytes

LOCAL_DEFAULT_HOST = "localhost"
LOCAL_DEFAULT_PORT = 6379


class RedisClient:
    def __init__(
        self,
        host: str,
        port: int,
        username: Optional[str],
        password: Optional[str],
        socket_timeout: int,
        socket_connect_timeout: int,
        health_check_interval: int,
        socket_keepalive: bool,
        retry_strategy: Retry,
    ) -> None:
        self._lock = asyncio.Lock()
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._socket_timeout = socket_timeout
        self._socket_connect_timeout = socket_connect_timeout
        self._health_check_interval = health_check_interval
        self._socket_keepalive = socket_keepalive
        self._retry_strategy = retry_strategy
        self._create_client()

    @classmethod
    def from_remote_defaults(cls) -> Self:
        return cls(
            host=EnvLoader.get("REDIS_HOST"),
            port=int(EnvLoader.get("REDIS_PORT")),
            username=EnvLoader.get("REDIS_USERNAME"),
            password=EnvLoader.get("REDIS_PASSWORD"),
            socket_timeout=20,
            socket_connect_timeout=10,
            health_check_interval=20,
            socket_keepalive=True,
            retry_strategy=Retry(
                backoff=ExponentialBackoff(),
                retries=3,
                supported_errors=(ConnectionError, RedisTimeoutError),
            ),
        )

    @classmethod
    def from_local_defaults(cls) -> Self:
        return cls(
            host=LOCAL_DEFAULT_HOST,
            port=LOCAL_DEFAULT_PORT,
            username=None,
            password=None,
            socket_timeout=10,
            socket_connect_timeout=2,
            health_check_interval=0,
            socket_keepalive=False,
            retry_strategy=Retry(
                backoff=ExponentialBackoff(),
                retries=2,
                supported_errors=(ConnectionError, RedisTimeoutError),
            ),
        )

    def _create_client(self) -> None:
        self.__connection_pool = ConnectionPool(
            host=self._host,
            port=self._port,
            username=self._username,
            password=self._password,
            socket_timeout=self._socket_timeout,
            socket_connect_timeout=self._socket_connect_timeout,
            health_check_interval=self._health_check_interval,
            retry=self._retry_strategy,
            decode_responses=True,
            socket_keepalive=self._socket_keepalive,
        )
        self.client = Redis(
            connection_pool=self.__connection_pool,
            decode_responses=True,
        )

    def __repr__(self) -> str:
        return f"<RedisClient host={self._host} port={self._port}>"

    async def _recreate_client(self) -> None:
        async with self._lock:
            try:
                await self.client.close()
            except Exception as e:
                logging.warning(f"Failed to close Redis client cleanly: {e}")
            self._create_client()

    async def safe_blpop(
        self, key: str, timeout: Optional[int | float] = 0
    ) -> Optional[tuple[str, str]]:
        try:
            return await self.client.blpop(key, timeout=timeout)
        except (ConnectionError, RedisTimeoutError) as e:
            logging.warning(f"Redis error on BLPOP: {e}. Recreating Redis client.")
            await self._recreate_client()
            try:
                return await self.client.blpop(key, timeout=timeout)
            except Exception as e2:
                logging.error(f"Retry after reconnect failed on BLPOP: {e2}")
                raise

    async def safe_rpush(self, name: str, *values: FieldT) -> int:
        try:
            return await self.client.rpush(name, *values)
        except (ConnectionError, RedisTimeoutError) as e:
            logging.warning(f"Redis error on RPUSH: {e}. Recreating Redis client.")
            await self._recreate_client()
            try:
                return await self.client.rpush(name, *values)
            except Exception as e2:
                logging.error(f"Retry after reconnect failed on RPUSH: {e2}")
                raise

    async def close(self) -> None:
        await self.client.close()
