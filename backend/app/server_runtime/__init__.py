"""Server-only runtime seams built on the transport-neutral Phase 0 contracts."""

from .health import CollectorHealth, CollectorState
from .leases import (
    StreamCheckpointError,
    StreamLease,
    StreamLeaseBusyError,
    StreamLeaseError,
    StreamLeaseFencedError,
    StreamLeaseStore,
)
from .producer_identity import ProducerIdentity
from .projection import (
    KafkaMarketEventRecord,
    MarketEventProjector,
    MarketEventRecordError,
    ProjectionBatchResult,
    ProjectionIntegrityError,
    decode_kafka_market_event,
)
from .projector_service import ClickHouseWriterService, ClickHouseWriterServiceError
from .service import (
    AggTradeCollectorService,
    CollectorGapError,
    CollectorServiceError,
    CollectorShutdownError,
)
from .settings import ServerCollectorConfigurationError, ServerCollectorSettings
from .writer_health import ClickHouseWriterHealth, ClickHouseWriterState
from .writer_settings import (
    DEFAULT_GROUP_ID,
    ClickHouseWriterConfigurationError,
    ClickHouseWriterSettings,
)

__all__ = [
    "DEFAULT_GROUP_ID",
    "AggTradeCollectorService",
    "ClickHouseWriterConfigurationError",
    "ClickHouseWriterHealth",
    "ClickHouseWriterService",
    "ClickHouseWriterServiceError",
    "ClickHouseWriterSettings",
    "ClickHouseWriterState",
    "CollectorGapError",
    "CollectorHealth",
    "CollectorServiceError",
    "CollectorShutdownError",
    "CollectorState",
    "KafkaMarketEventRecord",
    "MarketEventProjector",
    "MarketEventRecordError",
    "ProducerIdentity",
    "ProjectionBatchResult",
    "ProjectionIntegrityError",
    "ServerCollectorConfigurationError",
    "ServerCollectorSettings",
    "StreamCheckpointError",
    "StreamLease",
    "StreamLeaseBusyError",
    "StreamLeaseError",
    "StreamLeaseFencedError",
    "StreamLeaseStore",
    "decode_kafka_market_event",
]
