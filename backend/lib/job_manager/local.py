from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.data_models import JobStatus
from backend.env_loader import EnvLoader
from backend.lib.redis.client import RedisClient
from backend.worker.job_processor.local.types import (
    LocalJobInputPayload,
    LocalJobOutputPayload,
)

from .base import AbstractJobManager
from .types import LocalJobQueue, LocalJobType


class LocalJobManager(
    AbstractJobManager[
        LocalJobType,
        LocalJobInputPayload,
        LocalJobOutputPayload,
    ]
):
    @classmethod
    def build_queue_name(cls, queue: LocalJobQueue) -> str:
        prefix = (
            "LOCAL_DEV_"
            if EnvLoader.get_optional("ENV") != "production"
            else "LOCAL_PROD_"
        )
        return prefix + str(queue)

    def __init__(self, redis: RedisClient, queue: LocalJobQueue) -> None:
        queue_name = self.build_queue_name(queue)
        super().__init__(redis, queue_name)
        self._job_payload_map: dict[
            UUID, tuple[LocalJobType, LocalJobInputPayload]
        ] = {}

    async def enqueue(
        self,
        job_type: LocalJobType,
        job_payload: LocalJobInputPayload,
        db_session: Optional[AsyncSession] = None,
    ) -> UUID:
        job_id = uuid4()
        self._job_payload_map[job_id] = (job_type, job_payload)
        await self.redis.safe_rpush(self.queue_name, str(job_id))
        return job_id

    async def claim(
        self,
        job_id: UUID,
        db_session: Optional[AsyncSession] = None,
    ) -> tuple[LocalJobType, LocalJobInputPayload]:
        return self._job_payload_map[job_id]

    async def update_status(
        self,
        job_id: UUID,
        status: JobStatus,
        error_message: Optional[str] = None,
        result_payload: Optional[LocalJobOutputPayload] = None,
        db_session: Optional[AsyncSession] = None,
    ) -> None:
        # No-op: optionally log or record in memory
        pass
