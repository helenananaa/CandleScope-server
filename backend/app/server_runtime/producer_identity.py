"""Stable producer identity carried by every durable market event."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ProducerIdentity:
    """Application-level producer identity and fencing epoch."""

    producer_id: str
    producer_epoch: int

    def __post_init__(self) -> None:
        if not isinstance(self.producer_id, str):
            raise TypeError("producer_id must be a string")
        producer_id = self.producer_id.strip()
        if not producer_id:
            raise ValueError("producer_id cannot be blank")
        if isinstance(self.producer_epoch, bool) or not isinstance(
            self.producer_epoch,
            int,
        ):
            raise TypeError("producer_epoch must be an integer")
        if self.producer_epoch < 0:
            raise ValueError("producer_epoch must be non-negative")
        object.__setattr__(self, "producer_id", producer_id)
