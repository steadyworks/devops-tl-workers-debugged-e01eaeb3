import asyncio
import logging
from multiprocessing import Process
from multiprocessing.connection import Connection
from typing import cast
from uuid import UUID

from backend.db.data_models import JobStatus
from backend.db.session.factory import AsyncSessionFactory
from backend.lib.asset_manager.base import AssetManager
from backend.lib.asset_manager.factory import AssetManagerFactory
from backend.lib.job_manager.remote import RemoteJobManager
from backend.lib.job_manager.types import RemoteJobQueue, RemoteJobType
from backend.lib.redis.client import RedisClient
from backend.logging_utils import configure_logging_env
from backend.worker.job_processor.remote.factory import RemoteJobProcessorFactory
from backend.worker.job_processor.remote.types import (
    RemoteJobInputPayload,
    RemoteJobOutputPayload,
)

MAX_JOB_TIMEOUT_SECS = 600  # 10 mins
SEND_HEARTBEAT_EVERY_SECS = 1
POLL_SHUTDOWN_EVERY_SECS = 1
WORKER_CONCURRENCY = 6


class IOBoundWebWorkerProcess(Process):
    def __init__(self, heartbeat_connection: Connection, name: str = "worker"):
        # Runs in the parent process
        super().__init__()
        self.name = name
        self.heartbeat_connection = heartbeat_connection

    def run(self) -> None:
        # Run in child process

        try:
            configure_logging_env()
            logging.info(f"[{self.name}] Worker started with PID {self.pid}")

            # Initialize shutdown event
            shutdown_event = asyncio.Event()

            # Start main job defined in children classes
            remote_redis = RedisClient.from_remote_defaults()
            job_manager = RemoteJobManager(remote_redis, RemoteJobQueue.MAIN_TASK_QUEUE)
            asset_manager = AssetManagerFactory().create()
            db_session_factory = AsyncSessionFactory()

            async def _wrapped_main(
                _remote_redis: RedisClient,
                _job_manager: RemoteJobManager,
                _asset_manager: AssetManager,
                _db_session_factory: AsyncSessionFactory,
                _shutdown_event: asyncio.Event,
            ) -> None:
                try:
                    await self._supervised_main_loop(
                        _job_manager,
                        _asset_manager,
                        _db_session_factory,
                        _shutdown_event,
                    )
                finally:
                    await _remote_redis.close()

            asyncio.run(
                _wrapped_main(
                    remote_redis,
                    job_manager,
                    asset_manager,
                    db_session_factory,
                    shutdown_event,
                )
            )
        except Exception as e:
            logging.exception(f"[{self.name}] Worker crashed: {e}")

    async def _supervised_main_loop(
        self,
        job_manager: RemoteJobManager,
        asset_manager: AssetManager,
        db_session_factory: AsyncSessionFactory,
        shutdown_event: asyncio.Event,
    ) -> None:
        logging.info(f"[{self.name}] Started worker process (PID={self.pid})")

        # Launch all workers + monitor
        await asyncio.gather(
            self._heartbeat_ping_loop(shutdown_event),
            self._heartbeat_shutdown_monitor(shutdown_event),
            self._supervisor(
                job_manager, asset_manager, db_session_factory, shutdown_event
            ),
        )

        logging.info(f"[{self.name}] All tasks shut down cleanly")

    async def _heartbeat_ping_loop(
        self,
        shutdown_event: asyncio.Event,
    ) -> None:
        while not shutdown_event.is_set():
            try:
                self.heartbeat_connection.send("ping")
                await asyncio.sleep(SEND_HEARTBEAT_EVERY_SECS)
            except Exception:
                logging.warning(f"[{self.name}] Heartbeat pipe closed (send)")
                shutdown_event.set()
                break

    async def _heartbeat_shutdown_monitor(
        self,
        shutdown_event: asyncio.Event,
    ) -> None:
        while not shutdown_event.is_set():
            if self.heartbeat_connection.poll(timeout=POLL_SHUTDOWN_EVERY_SECS):
                try:
                    msg = self.heartbeat_connection.recv()
                    if msg == "shutdown":
                        logging.info(f"[{self.name}] Received shutdown signal")
                        shutdown_event.set()
                        break
                except EOFError:
                    logging.warning(f"[{self.name}] Heartbeat pipe closed")
                    shutdown_event.set()
                    break
            await asyncio.sleep(0.1)

    async def _supervisor(
        self,
        job_manager: RemoteJobManager,
        asset_manager: AssetManager,
        db_session_factory: AsyncSessionFactory,
        shutdown_event: asyncio.Event,
    ) -> None:
        running_tasks: dict[int, asyncio.Task[None]] = {}

        # Start all workers
        for i in range(WORKER_CONCURRENCY):
            running_tasks[i] = asyncio.create_task(
                self._spawn_worker_forever(
                    i, job_manager, asset_manager, db_session_factory, shutdown_event
                )
            )

        # Monitor loop
        while not shutdown_event.is_set():
            await asyncio.sleep(1)

            for i, task in list(running_tasks.items()):
                if task.done():
                    exc = task.exception()
                    if exc:
                        logging.error(
                            f"[{self.name}] Worker-{i} exited with error: {exc}"
                        )
                    else:
                        logging.warning(
                            f"[{self.name}] Worker-{i} exited cleanly (unexpected)"
                        )

                    # Restart
                    logging.info(f"[{self.name}] Restarting Worker-{i}")
                    running_tasks[i] = asyncio.create_task(
                        self._spawn_worker_forever(
                            i,
                            job_manager,
                            asset_manager,
                            db_session_factory,
                            shutdown_event,
                        )
                    )

        # Shutdown triggered: cancel all
        logging.info(f"[{self.name}] Cancelling all workers...")
        for task in running_tasks.values():
            task.cancel()

        await asyncio.gather(*running_tasks.values(), return_exceptions=True)
        logging.info(f"[{self.name}] All workers shut down cleanly")

    async def _spawn_worker_forever(
        self,
        i: int,
        job_manager: RemoteJobManager,
        asset_manager: AssetManager,
        db_session_factory: AsyncSessionFactory,
        shutdown_event: asyncio.Event,
    ) -> None:
        while not shutdown_event.is_set():
            try:
                logging.info(f"[{self.name}] Spawning worker-{i}")
                await self._job_worker_main_loop(
                    i,
                    job_manager,
                    asset_manager,
                    db_session_factory,
                    shutdown_event,
                )
            except Exception as e:
                logging.exception(
                    f"[{self.name}] Worker-{i} crashed: {e}. Restarting after delay."
                )
                await asyncio.sleep(1)  # optional backoff

    async def _job_worker_main_loop(
        self,
        worker_id: int,
        job_manager: RemoteJobManager,
        asset_manager: AssetManager,
        db_session_factory: AsyncSessionFactory,
        shutdown_event: asyncio.Event,
    ) -> None:
        while not shutdown_event.is_set():
            try:
                job_uuid = await job_manager.poll(timeout=5)
                if shutdown_event.is_set():
                    break
                if job_uuid is None:
                    continue

                await self._process_job_polled_from_redis(
                    job_uuid,
                    job_manager,
                    asset_manager,
                    db_session_factory,
                )
            except asyncio.CancelledError:
                logging.info(f"[{self.name}][Worker {worker_id}] Cancelled")
                raise
            except Exception:
                logging.exception(f"[{self.name}][Worker {worker_id}] Unexpected error")

    async def _process_job_polled_from_redis(
        self,
        job_uuid: UUID,
        job_manager: RemoteJobManager,
        asset_manager: AssetManager,
        db_session_factory: AsyncSessionFactory,
    ) -> None:
        job_type, job_input_payload = None, None
        try:
            async with db_session_factory.new_session() as db_session:
                job_type, job_input_payload = await job_manager.claim(
                    job_uuid, db_session=db_session
                )
        except asyncio.CancelledError:
            logging.info(f"[{self.name}] Cancelled while claiming job {job_uuid}")
            raise
        except Exception:
            logging.exception(
                f"[{self.name}] Job claim DB write failed for job: {job_uuid}"
            )
            await self._mark_job_as_error(
                job_manager,
                db_session_factory,
                job_uuid,
                "Failed to mark job as dequeued",
            )
            return  # Not successfully claimed

        try:
            await asyncio.wait_for(
                self._handle_task(
                    job_uuid,
                    job_type,
                    job_input_payload,
                    job_manager,
                    asset_manager,
                    db_session_factory,
                ),
                timeout=MAX_JOB_TIMEOUT_SECS,
            )
        except asyncio.CancelledError:
            logging.info(f"[{self.name}] Cancelled while running job {job_uuid}")
            raise
        except asyncio.TimeoutError:
            logging.warning(
                f"[{self.name}] Job timed out after {MAX_JOB_TIMEOUT_SECS}s, "
                f"job_id: {job_uuid} "
                f"payload: {job_input_payload.model_dump_json() if job_input_payload else '<missing payload>'}"
            )
            await self._mark_job_as_error(
                job_manager,
                db_session_factory,
                job_uuid,
                f"Timeout after {MAX_JOB_TIMEOUT_SECS}s",
            )
        except Exception as e:
            logging.warning(
                f"[{self.name}] Job failed: job_id: {job_uuid} payload: "
                f"payload: {job_input_payload.model_dump_json() if job_input_payload else '<missing payload>'}"
            )
            await self._mark_job_as_error(
                job_manager,
                db_session_factory,
                job_uuid,
                f"Job execution failed due to {str(e)}",
            )

    async def _handle_task(
        self,
        job_uuid: UUID,
        job_type: RemoteJobType,
        job_input_payload: RemoteJobInputPayload,
        job_manager: RemoteJobManager,
        asset_manager: AssetManager,
        db_session_factory: AsyncSessionFactory,
    ) -> None:
        try:
            async with db_session_factory.new_session() as db_session:
                await job_manager.update_status(
                    job_uuid, JobStatus.PROCESSING, db_session=db_session
                )

            job_processor = RemoteJobProcessorFactory.new_processor(
                job_uuid, job_type, asset_manager, db_session_factory
            )
            result = cast(
                "RemoteJobOutputPayload", await job_processor.process(job_input_payload)
            )

            async with db_session_factory.new_session() as db_session:
                await job_manager.update_status(
                    job_uuid,
                    JobStatus.DONE,
                    result_payload=result,
                    db_session=db_session,
                )
        except asyncio.CancelledError:
            logging.info(f"[{self.name}] Cancelled while processing job {job_uuid}")
            raise
        except Exception as e:
            logging.warning(f"[{self.name}] Failed job {job_uuid}: {e}")
            raise e

    async def _mark_job_as_error(
        self,
        job_manager: RemoteJobManager,
        db_session_factory: AsyncSessionFactory,
        job_uuid: UUID,
        reason: str,
    ) -> None:
        try:
            async with db_session_factory.new_session() as db_session:
                await job_manager.update_status(
                    job_uuid,
                    JobStatus.ERROR,
                    error_message=reason,
                    db_session=db_session,
                )
        except Exception as inner:
            logging.warning(
                f"[{self.name}] Failed to mark job {job_uuid} as error: {inner}"
            )
