"""Provision or verify the `rag-diagnostic-traces` Kafka topic (32 partitions)."""

from __future__ import annotations

import asyncio
import sys

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from internal.config.settings import WorkerSettings
from internal.consumer.kafka_consumer import build_kafka_client_kwargs
from internal.telemetry.logger import configure_logging, get_logger


async def _verify_existing_topic(
    admin: AIOKafkaAdminClient,
    settings: WorkerSettings,
    logger: object,
) -> None:
    metadata = await admin.describe_topics([settings.kafka_topic])
    actual = len(metadata[0]["partitions"]) if metadata else 0
    if actual != settings.kafka_topic_partitions:
        logger.warning(  # type: ignore[attr-defined]
            "kafka_topic_partition_mismatch",
            topic=settings.kafka_topic,
            expected_partitions=settings.kafka_topic_partitions,
            actual_partitions=actual,
        )
        sys.exit(1)
    logger.info(  # type: ignore[attr-defined]
        "kafka_topic_verified",
        topic=settings.kafka_topic,
        partitions=actual,
    )


async def ensure_kafka_topic(settings: WorkerSettings) -> None:
    """Create `rag-diagnostic-traces` with 32 partitions if absent and verify partition count."""
    logger = get_logger("ensure_kafka_topic", stage="admin")
    admin = AIOKafkaAdminClient(**build_kafka_client_kwargs(settings))
    await admin.start()
    try:
        topic = NewTopic(
            name=settings.kafka_topic,
            num_partitions=settings.kafka_topic_partitions,
            replication_factor=1,
        )
        try:
            response = await admin.create_topics([topic])
        except TopicAlreadyExistsError:
            await _verify_existing_topic(admin, settings, logger)
            return

        topic_errors = getattr(response, "topic_errors", None) or []
        for topic_name, error_code, _error_message in topic_errors:
            if error_code == 0:
                continue
            if error_code == TopicAlreadyExistsError.errno:
                await _verify_existing_topic(admin, settings, logger)
                return
            logger.error(
                "kafka_topic_creation_failed",
                topic=topic_name,
                error_code=error_code,
            )
            sys.exit(1)

        logger.info(
            "kafka_topic_created",
            topic=settings.kafka_topic,
            partitions=settings.kafka_topic_partitions,
        )
    finally:
        await admin.close()


def main() -> None:
    settings = WorkerSettings()
    configure_logging(settings.log_level)
    asyncio.run(ensure_kafka_topic(settings))


if __name__ == "__main__":
    main()
