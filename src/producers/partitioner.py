"""Custom partitioning strategies for Kafka producers.

Demonstrates how to route messages to specific partitions based on business logic
rather than relying on default hash-based partitioning.
"""

from __future__ import annotations

import hashlib
import struct
from typing import Optional

import structlog

from src.models import Region

logger = structlog.get_logger(__name__)

# Region-to-partition mapping for data locality
# With 12 partitions, we allocate ranges per region
REGION_PARTITION_MAP: dict[Region, tuple[int, int]] = {
    Region.US_EAST: (0, 2),      # partitions 0-2
    Region.US_WEST: (3, 5),      # partitions 3-5
    Region.EU_WEST: (6, 7),      # partitions 6-7
    Region.EU_CENTRAL: (8, 9),   # partitions 8-9
    Region.APAC: (10, 11),       # partitions 10-11
}


class RegionAwarePartitioner:
    """Partition messages by customer region for data locality.

    Orders from the same region land on the same partition range,
    enabling region-local consumers and reducing cross-datacenter traffic.
    Within a region, we sub-partition by customer_id for even distribution.
    """

    def __init__(self, num_partitions: int = 12) -> None:
        self.num_partitions = num_partitions

    def __call__(
        self,
        key: Optional[bytes],
        all_partitions: list[int],
        available_partitions: list[int],
        region: Optional[Region] = None,
    ) -> int:
        """Determine partition for a message.

        Args:
            key: Message key (customer_id as bytes)
            all_partitions: All topic partitions
            available_partitions: Currently available partitions
            region: Customer region for locality-aware routing

        Returns:
            Target partition number
        """
        if region is None or key is None:
            # Fallback to murmur2-compatible hash (Kafka default)
            return self._default_partition(key, available_partitions)

        partition_range = REGION_PARTITION_MAP.get(region)
        if partition_range is None:
            return self._default_partition(key, available_partitions)

        start, end = partition_range
        # Sub-partition within region range using consistent hashing
        range_size = end - start + 1
        key_hash = self._murmur2_hash(key)
        partition = start + (key_hash % range_size)

        # If target partition is unavailable, pick closest available in range
        if partition not in available_partitions:
            for offset in range(1, range_size):
                candidate = start + ((key_hash + offset) % range_size)
                if candidate in available_partitions:
                    partition = candidate
                    break
            else:
                # All region partitions unavailable, fall back to any available
                partition = self._default_partition(key, available_partitions)

        logger.debug(
            "partitioned_message",
            region=region.value,
            partition=partition,
            key=key.decode() if key else None,
        )
        return partition

    def _default_partition(
        self, key: Optional[bytes], available_partitions: list[int]
    ) -> int:
        """Default partitioning using murmur2 hash."""
        if key is None:
            # Round-robin for null keys (sticky partitioner behavior)
            import random
            return random.choice(available_partitions)

        key_hash = self._murmur2_hash(key)
        return available_partitions[key_hash % len(available_partitions)]

    @staticmethod
    def _murmur2_hash(data: bytes) -> int:
        """Murmur2 hash compatible with Kafka's default partitioner.

        This matches the Java implementation used by the Kafka broker
        for consistent partition assignment across language clients.
        """
        length = len(data)
        seed = 0x9747B28C
        m = 0x5BD1E995
        r = 24

        h = seed ^ length
        length4 = length // 4

        for i in range(length4):
            i4 = i * 4
            k = (
                (data[i4] & 0xFF)
                + ((data[i4 + 1] & 0xFF) << 8)
                + ((data[i4 + 2] & 0xFF) << 16)
                + ((data[i4 + 3] & 0xFF) << 24)
            )
            k &= 0xFFFFFFFF
            k = (k * m) & 0xFFFFFFFF
            k ^= (k >> r) & 0xFFFFFFFF
            k = (k * m) & 0xFFFFFFFF
            h = (h * m) & 0xFFFFFFFF
            h ^= k

        remaining = length % 4
        tail_index = length4 * 4
        if remaining >= 3:
            h ^= (data[tail_index + 2] & 0xFF) << 16
        if remaining >= 2:
            h ^= (data[tail_index + 1] & 0xFF) << 8
        if remaining >= 1:
            h ^= data[tail_index] & 0xFF
            h = (h * m) & 0xFFFFFFFF

        h ^= (h >> 13) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= (h >> 15) & 0xFFFFFFFF

        return h & 0x7FFFFFFF  # Ensure positive


class PriorityPartitioner:
    """Route high-priority orders to dedicated partitions.

    High-value orders (>$1000) go to partition 0 for priority processing.
    This ensures premium customers get faster order confirmation.
    """

    PRIORITY_PARTITION = 0
    PRIORITY_THRESHOLD = 1000.0

    def __call__(
        self,
        key: Optional[bytes],
        all_partitions: list[int],
        available_partitions: list[int],
        order_amount: float = 0.0,
    ) -> int:
        if order_amount >= self.PRIORITY_THRESHOLD:
            if self.PRIORITY_PARTITION in available_partitions:
                return self.PRIORITY_PARTITION

        # Standard hash partitioning for non-priority
        if key is not None:
            hash_val = int(hashlib.md5(key).hexdigest(), 16)
            # Skip partition 0 (reserved for priority)
            non_priority = [p for p in available_partitions if p != self.PRIORITY_PARTITION]
            if non_priority:
                return non_priority[hash_val % len(non_priority)]

        import random
        return random.choice(available_partitions)
