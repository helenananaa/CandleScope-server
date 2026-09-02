"""Standalone Phase 1H snapshot query HTTP service entrypoint."""

from __future__ import annotations

import logging
import os

import uvicorn
from app.server_runtime.query_api import create_snapshot_query_app
from app.server_runtime.query_cursor import KafkaProjectionCursorReader
from app.server_runtime.query_router import SnapshotQueryRouter
from app.server_runtime.query_security import (
    BearerTokenAuthenticator,
    StructuredLogQueryAuditSink,
)
from app.server_runtime.query_settings import QueryServiceSettings
from app.server_runtime.storage.clickhouse_query import (
    ClickHouseSnapshotMarketEventQuery,
)
from app.server_runtime.storage.parquet_archive import (
    ImmutableParquetMarketEventArchive,
    ParquetMarketEventQuery,
)
from app.server_runtime.storage.postgres_query_control import (
    PostgresQueryControlStore,
)
from app.server_runtime.storage.s3 import S3ImmutableObjectStore


def build_app(settings: QueryServiceSettings):
    control_store: PostgresQueryControlStore | None = None
    audit_sink = StructuredLogQueryAuditSink()
    control_authenticator: BearerTokenAuthenticator | None = None
    if settings.control_backend == "postgres":
        if settings.postgres_dsn is None or settings.control_bearer_token is None:
            raise RuntimeError("validated PostgreSQL control settings are missing")
        control_store = PostgresQueryControlStore(
            settings.postgres_dsn,
            instance_id=settings.instance_id,
            backend_id=settings.hot_backend_id,
            connect_timeout_ms=settings.postgres_connect_timeout_ms,
            request_timeout_ms=settings.postgres_request_timeout_ms,
        )
        audit_sink = control_store
        control_authenticator = BearerTokenAuthenticator(
            token=settings.control_bearer_token,
            principal=settings.control_principal,
        )
    store = S3ImmutableObjectStore(
        endpoint_url=settings.s3_endpoint_url,
        region=settings.s3_region,
        bucket=settings.s3_bucket,
        prefix=settings.s3_prefix,
        access_key_id=settings.s3_access_key_id,
        secret_access_key=settings.s3_secret_access_key,
        request_timeout_ms=settings.s3_request_timeout_ms,
    )
    archive = ImmutableParquetMarketEventArchive(
        object_store=store,
        max_manifest_depth=settings.max_manifest_depth,
    )
    router = SnapshotQueryRouter(
        cold_query=ParquetMarketEventQuery(
            archive=archive,
            max_page_rows=settings.max_page_rows,
        ),
        hot_query=ClickHouseSnapshotMarketEventQuery(
            url=settings.clickhouse_url,
            database=settings.clickhouse_database,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            request_timeout_ms=settings.clickhouse_request_timeout_ms,
            max_scan_rows=settings.max_scan_rows,
            max_page_rows=settings.max_page_rows,
            max_execution_time_ms=settings.clickhouse_max_execution_time_ms,
            max_memory_usage_bytes=settings.clickhouse_max_memory_usage_bytes,
            max_bytes_to_read=settings.clickhouse_max_bytes_to_read,
            max_threads=settings.clickhouse_max_threads,
        ),
        projection_cursor=KafkaProjectionCursorReader(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.clickhouse_writer_group_id,
            client_id=f"candlescope-phase1h-query-cursor-{settings.instance_id}",
            connection_options={
                "request_timeout_ms": settings.kafka_request_timeout_ms,
            },
        ),
        quarantine_store=control_store,
        parity_sample_interval_ms=settings.parity_sample_interval_ms,
        parity_probe_capacity=settings.parity_probe_capacity,
    )
    return create_snapshot_query_app(
        router=router,
        authenticator=BearerTokenAuthenticator(
            token=settings.auth_bearer_token,
            principal=settings.auth_principal,
            organization_id=settings.auth_organization_id,
            workspace_id=settings.auth_workspace_id,
        ),
        audit_sink=audit_sink,
        control_authenticator=control_authenticator,
        max_concurrent_queries=settings.max_concurrent_queries,
        query_queue_timeout_ms=settings.query_queue_timeout_ms,
    )


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("CANDLESCOPE_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = QueryServiceSettings.from_env()
    uvicorn.run(
        build_app(settings),
        host=settings.bind_host,
        port=settings.bind_port,
        log_level=os.environ.get("CANDLESCOPE_LOG_LEVEL", "INFO").lower(),
    )


if __name__ == "__main__":
    main()
