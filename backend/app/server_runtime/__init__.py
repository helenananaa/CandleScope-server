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
from .service import (
    AggTradeCollectorService,
    CollectorGapError,
    CollectorServiceError,
    CollectorShutdownError,
)
from .settings import ServerCollectorConfigurationError, ServerCollectorSettings

__all__ = [
    "AggTradeCollectorService",
    "CollectorGapError",
    "CollectorHealth",
    "CollectorServiceError",
    "CollectorShutdownError",
    "CollectorState",
    "ProducerIdentity",
    "ServerCollectorConfigurationError",
    "ServerCollectorSettings",
    "StreamCheckpointError",
    "StreamLease",
    "StreamLeaseBusyError",
    "StreamLeaseError",
    "StreamLeaseFencedError",
    "StreamLeaseStore",
]
