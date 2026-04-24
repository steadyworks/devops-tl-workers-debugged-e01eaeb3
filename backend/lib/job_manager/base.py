from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.data_models import JobStatus
from backend.lib.redis.client import RedisClient

# Define base job payload and output payload types
TJobInput = TypeVar("TJobInput")
TJobOutput = TypeVar("TJobOutput")
TJobType = TypeVar("TJobType")

DEFAULT_DEQUEUE_POLL_TIMEOUT_SECS = 5


class AbstractJobManager(ABC, Generic[TJobType, TJobInput, TJobOutput]):
    def __init__(self, redis: RedisClient, queue_name: str) -> None:
        self.redis = redis
        self.queue_name = queue_name

    async def poll(
        self,
        timeout: Optional[int] = DEFAULT_DEQUEUE_POLL_TIMEOUT_SECS,
    ) -> Optional[UUID]:
        result = await self.redis.safe_blpop(self.queue_name, timeout=timeout)
        if not result:
            return None  # timeout occurred

        _queue_name, job_id_str = result
        try:
            return UUID(job_id_str)
        except ValueError:
            return None

    @abstractmethod
    async def enqueue(
        self,
        job_type: TJobType,
        job_payload: TJobInput,
        db_session: Optional[AsyncSession] = None,
    ) -> UUID: ...

    @abstractmethod
    async def claim(
        self,
        job_id: UUID,
        db_session: Optional[AsyncSession] = None,
    ) -> tuple[TJobType, TJobInput]: ...

    @abstractmethod
    async def update_status(
        self,
        job_id: UUID,
        status: JobStatus,
        error_message: Optional[str] = None,
        result_payload: Optional[TJobOutput] = None,
        db_session: Optional[AsyncSession] = None,
    ) -> None: ...
