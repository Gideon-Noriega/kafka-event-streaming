"""Fraud detection - windowed aggregation pattern."""
from __future__ import annotations
import time, uuid
from datetime import timedelta
import faust
from src.streams.app import app


class OrderEvent(faust.Record):
    order_id: str
    customer_id: str
    total_amount: float
    region: str
    placed_at: float


class FraudAlert(faust.Record):
    alert_id: str
    customer_id: str
    order_count: int
    total_amount: float
    window_start: float
    window_end: float
    risk_score: float
    reason: str


class CustomerOrderStats(faust.Record):
    order_count: int = 0
    total_amount: float = 0.0
    regions: list = None

    def __post_init__(self):
        if self.regions is None:
            self.regions = []


orders_topic = app.topic("orders.placed", value_type=OrderEvent)
alerts_topic = app.topic("orders.fraud_alerts", value_type=FraudAlert)
ORDER_WINDOW = timedelta(minutes=5)
MAX_ORDERS = 10
MAX_AMOUNT = 5000.0
MAX_REGIONS = 3


class FraudDetectionStream:
    """Detect suspicious patterns: frequency, spend, impossible travel."""

    customer_stats = app.Table(
        "customer-order-stats", default=CustomerOrderStats,
    ).tumbling(ORDER_WINDOW, expires=timedelta(minutes=30))

    @staticmethod
    @app.agent(orders_topic)
    async def detect_fraud(stream):
        """Aggregate orders per customer in tumbling windows."""
        async for order in stream:
            stats = FraudDetectionStream.customer_stats[order.customer_id].value()
            stats.order_count += 1
            stats.total_amount += order.total_amount
            if order.region not in stats.regions:
                stats.regions.append(order.region)
            FraudDetectionStream.customer_stats[order.customer_id] = stats

            risk_score = 0.0
            reasons = []
            if stats.order_count > MAX_ORDERS:
                risk_score += 0.4
                reasons.append(f"high_freq:{stats.order_count}")
            if stats.total_amount > MAX_AMOUNT:
                risk_score += 0.3
                reasons.append(f"high_spend:${stats.total_amount:.0f}")
            if len(stats.regions) > MAX_REGIONS:
                risk_score += 0.3
                reasons.append(f"multi_region:{len(stats.regions)}")

            if risk_score >= 0.4:
                alert = FraudAlert(
                    alert_id=str(uuid.uuid4()), customer_id=order.customer_id,
                    order_count=stats.order_count, total_amount=stats.total_amount,
                    window_start=time.time() - 300, window_end=time.time(),
                    risk_score=min(risk_score, 1.0), reason="; ".join(reasons))
                await alerts_topic.send(key=order.customer_id, value=alert)
