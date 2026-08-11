"""Persistent adapters for the server control plane."""

from .clickhouse import (
    MARKET_EVENT_CONFLICT_TABLE,
    MARKET_EVENT_FACT_TABLE,
    ClickHouseHttpError,
    ClickHouseMarketEventProjector,
    ClickHouseProjectionError,
    ClickHouseSchemaError,
)
from .postgres_lease import (
    CREATE_STREAM_LEASE_TABLE_SQL,
    STREAM_LEASE_TABLE,
    PostgresStreamLeaseStore,
)
from .postgres_query_control import (
    HOT_PROJECTION_QUARANTINE_TABLE,
    QUERY_AUDIT_EVENT_TABLE,
    QUERY_AUDIT_HEAD_TABLE,
    PostgresQueryAuditVerifier,
    PostgresQueryControlStore,
    QueryAuditChainVerification,
)

__all__ = [
    "CREATE_STREAM_LEASE_TABLE_SQL",
    "HOT_PROJECTION_QUARANTINE_TABLE",
    "MARKET_EVENT_CONFLICT_TABLE",
    "MARKET_EVENT_FACT_TABLE",
    "QUERY_AUDIT_EVENT_TABLE",
    "QUERY_AUDIT_HEAD_TABLE",
    "STREAM_LEASE_TABLE",
    "ClickHouseHttpError",
    "ClickHouseMarketEventProjector",
    "ClickHouseProjectionError",
    "ClickHouseSchemaError",
    "PostgresStreamLeaseStore",
    "PostgresQueryAuditVerifier",
    "PostgresQueryControlStore",
    "QueryAuditChainVerification",
]
