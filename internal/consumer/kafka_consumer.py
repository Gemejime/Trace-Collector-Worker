"""Process-isolated Kafka consumer for rag-diagnostic-traces (ADR-001)."""

from __future__ import annotations

import asyncio
import inspect
import ssl
import time
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from aiokafka import (
    AIOKafkaConsumer,
    ConsumerRebalanceListener,
    OffsetAndMetadata,
    TopicPartition,
)
from google.protobuf.message import DecodeError

from api.proto.v1.trace_record_pb2 import TraceSpanRecord
from internal.config.settings import WorkerSettings
from internal.consumer.offset_manager import OffsetManager
from internal.telemetry.logger import get_logger

SpanHandler = Callable[[TopicPartition, int, TraceSpanRecord], Awaitable[None]]
RevocationHook = Callable[[Iterable[TopicPartition]], Awaitable[None] | None]


def build_ssl_context(settings: WorkerSettings) -> ssl.SSLContext | None:
    """Build an SSLContext when Kafka security protocol requires TLS."""
    if settings.kafka_security_protocol not in ("SSL", "SASL_SSL"):
        return None
    ctx = ssl.create_default_context(cafile=settings.kafka_ssl_cafile)
    if settings.kafka_ssl_certfile and settings.kafka_ssl_keyfile:
        key_password = (
            settings.kafka_ssl_key_password.get_secret_value()
            if settings.kafka_ssl_key_password is not None
            else None
        )
        ctx.load_cert_chain(
            certfile=settings.kafka_ssl_certfile,
            keyfile=settings.kafka_ssl_keyfile,
            password=key_password,
        )
    return ctx


def build_kafka_client_kwargs(settings: WorkerSettings) -> dict[str, Any]:
    """Build shared Kafka connection and security kwargs for consumer and admin clients."""
    ssl_context = build_ssl_context(settings)
    kwargs: dict[str, Any] = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "security_protocol": settings.kafka_security_protocol,
    }
    if ssl_context is not None:
        kwargs["ssl_context"] = ssl_context
    if settings.kafka_security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        kwargs["sasl_mechanism"] = settings.kafka_sasl_mechanism
        kwargs["sasl_plain_username"] = settings.kafka_sasl_username
        kwargs["sasl_plain_password"] = (
            settings.kafka_sasl_password.get_secret_value()
            if settings.kafka_sasl_password is not None
            else None
        )
    return kwargs


class _RebalanceListener(ConsumerRebalanceListener):  # type: ignore[misc]
    """Commits durable contiguous offsets before partition revocation."""

    def __init__(self, owner: KafkaSpanConsumer) -> None:
        self._owner = owner

    async def on_partitions_revoked(self, revoked: Iterable[TopicPartition]) -> None:
        await self._owner.handle_partitions_revoked(revoked)

    async def on_partitions_assigned(self, assigned: Iterable[TopicPartition]) -> None:
        self._owner.handle_partitions_assigned(assigned)


class KafkaSpanConsumer:
    """Consumes TraceSpanRecord payloads with bounded memory and contiguous offsets."""

    def __init__(
        self,
        settings: WorkerSettings,
        offset_manager: OffsetManager,
        span_handler: SpanHandler,
        consumer: AIOKafkaConsumer | None = None,
        on_revoked_hook: RevocationHook | None = None,
    ) -> None:
        if span_handler is None:
            raise ValueError("span_handler must be provided for durable offset handling")
        self._settings = settings
        self._offset_manager = offset_manager
        self._span_handler = span_handler
        self._consumer = consumer
        self._on_revoked_hook = on_revoked_hook
        self._owns_consumer = consumer is None
        self._logger = get_logger("kafka_consumer", stage="ingress")
        self._paused = False
        self._last_commit_ts = time.monotonic()
        self.consumed_count = 0
        self.quarantined_count = 0

    def _create_consumer(self) -> AIOKafkaConsumer:
        kwargs: dict[str, Any] = {
            **build_kafka_client_kwargs(self._settings),
            "group_id": self._settings.kafka_group_id,
            "auto_offset_reset": self._settings.kafka_auto_offset_reset,
            "enable_auto_commit": False,
            "max_poll_records": self._settings.kafka_max_poll_records,
            "fetch_max_bytes": self._settings.kafka_fetch_max_bytes,
            "max_partition_fetch_bytes": self._settings.kafka_max_partition_fetch_bytes,
            "max_poll_interval_ms": self._settings.kafka_max_poll_interval_ms,
            "session_timeout_ms": self._settings.kafka_session_timeout_ms,
            "heartbeat_interval_ms": self._settings.kafka_heartbeat_interval_ms,
        }
        return AIOKafkaConsumer(**kwargs)

    async def start(self) -> None:
        """Start the Kafka consumer, subscribe to the trace topic, and validate partitions."""
        if self._consumer is None:
            self._consumer = self._create_consumer()
        await self._consumer.start()
        listener = _RebalanceListener(self)
        self._consumer.subscribe([self._settings.kafka_topic], listener=listener)

        partitions = self._consumer.partitions_for_topic(self._settings.kafka_topic)
        if partitions is not None and len(partitions) != self._settings.kafka_topic_partitions:
            self._logger.warning(
                "kafka_topic_partition_count_mismatch",
                topic=self._settings.kafka_topic,
                expected_partitions=self._settings.kafka_topic_partitions,
                actual_partitions=len(partitions),
            )
        self._logger.info(
            "kafka_consumer_started",
            topic=self._settings.kafka_topic,
            group_id=self._settings.kafka_group_id,
            security_protocol=self._settings.kafka_security_protocol,
        )

    async def stop(self) -> None:
        """Commit remaining durable contiguous offsets and close the consumer."""
        if self._consumer is None:
            return
        try:
            await self.commit_durable_offsets()
        except Exception as exc:
            self._logger.warning(
                "kafka_final_commit_failed",
                error=type(exc).__name__,
            )
        finally:
            await self._consumer.stop()
            self._logger.info(
                "kafka_consumer_stopped",
                consumed_count=self.consumed_count,
                quarantined_count=self.quarantined_count,
            )

    def handle_partitions_assigned(self, assigned: Iterable[TopicPartition]) -> None:
        """Initialize offset tracking for newly assigned partitions."""
        assigned_list = list(assigned)
        self._offset_manager.on_partitions_assigned(assigned_list)
        self._logger.info(
            "kafka_partitions_assigned",
            partitions=[tp.partition for tp in assigned_list],
        )

    async def handle_partitions_revoked(self, revoked: Iterable[TopicPartition]) -> None:
        """Drop open DAG state and commit contiguous durable offsets for revoked partitions."""
        revoked_list = list(revoked)
        if self._on_revoked_hook is not None:
            hook_result = self._on_revoked_hook(revoked_list)
            if inspect.isawaitable(hook_result):
                await hook_result
        final_commits = self._offset_manager.on_partitions_revoked(revoked_list)
        if final_commits and self._consumer is not None:
            try:
                await self._consumer.commit(final_commits)
            except Exception as exc:
                self._logger.warning(
                    "kafka_revocation_commit_failed",
                    error=type(exc).__name__,
                )
        self._paused = False
        self._logger.info(
            "kafka_partitions_revoked",
            partitions=[tp.partition for tp in revoked_list],
        )

    async def poll_once(self, timeout_ms: int = 250) -> int:
        """Fetch one bounded batch of records, validate Protobufs, and update backpressure."""
        if self._consumer is None:
            raise RuntimeError("KafkaSpanConsumer must be started before polling")

        batch = await self._consumer.getmany(
            timeout_ms=timeout_ms,
            max_records=self._settings.kafka_max_poll_records,
        )
        processed = 0
        for tp, records in batch.items():
            for msg in records:
                processed += 1
                await self._process_message(tp, msg.offset, msg.value)

        self._update_backpressure()
        await self.commit_if_due()
        return processed

    async def _process_message(
        self,
        tp: TopicPartition,
        offset: int,
        payload: bytes | None,
    ) -> None:
        self._offset_manager.track_fetched(tp, offset)

        if payload is None or len(payload) > self._settings.kafka_max_payload_bytes:
            self._offset_manager.mark_quarantined(tp, offset)
            self.quarantined_count += 1
            self._logger.warning(
                "kafka_payload_quarantined",
                partition=tp.partition,
                offset=offset,
                payload_bytes=len(payload) if payload is not None else 0,
                reason="oversized_or_empty_payload",
            )
            return

        span = TraceSpanRecord()
        try:
            span.ParseFromString(payload)
        except DecodeError:
            self._offset_manager.mark_quarantined(tp, offset)
            self.quarantined_count += 1
            self._logger.warning(
                "kafka_payload_quarantined",
                partition=tp.partition,
                offset=offset,
                payload_bytes=len(payload),
                reason="protobuf_decode_error",
            )
            return

        if not span.trace_id or not span.span_id or not span.tenant_id:
            self._offset_manager.mark_quarantined(tp, offset)
            self.quarantined_count += 1
            self._logger.warning(
                "kafka_payload_quarantined",
                partition=tp.partition,
                offset=offset,
                reason="missing_required_span_fields",
            )
            return

        self.consumed_count += 1
        await self._span_handler(tp, offset, span)

    def _update_backpressure(self) -> None:
        if self._consumer is None:
            return
        in_flight = self._offset_manager.total_in_flight()
        high_water = self._settings.kafka_max_in_flight_offsets
        low_water = max(1, int(high_water * 0.7))

        if not self._paused and in_flight >= high_water:
            assignment = self._consumer.assignment()
            if assignment:
                self._consumer.pause(*assignment)
                self._paused = True
                self._logger.warning(
                    "kafka_consumer_paused_backpressure",
                    in_flight=in_flight,
                    high_water=high_water,
                )
        elif self._paused and in_flight <= low_water:
            paused = self._consumer.paused()
            if paused:
                self._consumer.resume(*paused)
            self._paused = False
            self._logger.info(
                "kafka_consumer_resumed_backpressure",
                in_flight=in_flight,
                low_water=low_water,
            )

    async def commit_if_due(self) -> dict[TopicPartition, OffsetAndMetadata]:
        """Commit durable contiguous offsets when the commit interval has elapsed."""
        now = time.monotonic()
        if (now - self._last_commit_ts) < self._settings.kafka_offset_commit_interval_seconds:
            return {}
        self._last_commit_ts = now
        return await self.commit_durable_offsets()

    async def commit_durable_offsets(self) -> dict[TopicPartition, OffsetAndMetadata]:
        """Commit all currently contiguous durable offsets to Kafka."""
        if self._consumer is None:
            return {}
        committable = self._offset_manager.committable_offsets()
        if not committable:
            return {}
        await self._consumer.commit(committable)
        self._offset_manager.mark_committed(committable)
        return committable

    async def run_until_stopped(self, stop_check: Callable[[], bool]) -> None:
        """Continuously poll Kafka until ``stop_check()`` returns True."""
        while not stop_check():
            processed = await self.poll_once(timeout_ms=250)
            if processed == 0:
                await asyncio.sleep(0.01)
