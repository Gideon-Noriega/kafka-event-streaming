"""Tests for producer implementations."""
import pytest
from src.models import Region
from src.producers.partitioner import RegionAwarePartitioner, PriorityPartitioner


class TestRegionAwarePartitioner:
    def setup_method(self):
        self.p = RegionAwarePartitioner(num_partitions=12)
        self.all = list(range(12))

    @pytest.mark.unit
    def test_us_east_routes_0_2(self):
        r = self.p(key=b"cust-123", all_partitions=self.all,
            available_partitions=self.all, region=Region.US_EAST)
        assert 0 <= r <= 2

    @pytest.mark.unit
    def test_us_west_routes_3_5(self):
        r = self.p(key=b"cust-456", all_partitions=self.all,
            available_partitions=self.all, region=Region.US_WEST)
        assert 3 <= r <= 5

    @pytest.mark.unit
    def test_eu_west_routes_6_7(self):
        r = self.p(key=b"cust-789", all_partitions=self.all,
            available_partitions=self.all, region=Region.EU_WEST)
        assert 6 <= r <= 7

    @pytest.mark.unit
    def test_apac_routes_10_11(self):
        r = self.p(key=b"cust-apac", all_partitions=self.all,
            available_partitions=self.all, region=Region.APAC)
        assert 10 <= r <= 11

    @pytest.mark.unit
    def test_deterministic(self):
        results = set()
        for _ in range(100):
            results.add(self.p(key=b"consistent", all_partitions=self.all,
                available_partitions=self.all, region=Region.US_EAST))
        assert len(results) == 1

    @pytest.mark.unit
    def test_null_key(self):
        r = self.p(key=None, all_partitions=self.all,
            available_partitions=self.all, region=None)
        assert r in self.all


class TestPriorityPartitioner:
    def setup_method(self):
        self.p = PriorityPartitioner()
        self.all = list(range(12))

    @pytest.mark.unit
    def test_high_value_priority(self):
        r = self.p(key=b"vip", all_partitions=self.all,
            available_partitions=self.all, order_amount=1500.0)
        assert r == 0

    @pytest.mark.unit
    def test_normal_avoids_priority(self):
        r = self.p(key=b"reg", all_partitions=self.all,
            available_partitions=self.all, order_amount=50.0)
        assert r != 0
