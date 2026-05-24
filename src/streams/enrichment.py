"""Order enrichment - KStream-KTable join pattern."""
from __future__ import annotations
import time
import faust
from src.streams.app import app


class OrderRecord(faust.Record):
    order_id: str
    customer_id: str
    items: list
    total_amount: float
    currency: str = "USD"
    payment_method: str = "credit_card"
    shipping_address: str = ""
    region: str = "us-east"
    placed_at: float = 0.0


class CustomerRecord(faust.Record):
    customer_id: str
    email: str = ""
    name: str = ""
    region: str = "us-east"
    tier: str = "standard"
    lifetime_orders: int = 0


class EnrichedOrderRecord(faust.Record):
    order_id: str
    customer_id: str
    customer_name: str
    customer_email: str
    customer_tier: str
    items: list
    total_amount: float
    currency: str
    is_high_value: bool
    region: str
    enriched_at: float


orders_topic = app.topic("orders.placed", value_type=OrderRecord)
customers_topic = app.topic("customers.profiles", value_type=CustomerRecord)
enriched_topic = app.topic("orders.enriched", value_type=EnrichedOrderRecord)
customer_table = app.Table("customer-profiles", default=CustomerRecord, partitions=6)


class OrderEnrichmentStream:
    """Enriches orders by joining with customer profile KTable."""

    @staticmethod
    @app.agent(customers_topic)
    async def track_customers(stream):
        """Maintain customer KTable from profile updates."""
        async for event in stream:
            customer_table[event.customer_id] = event

    @staticmethod
    @app.agent(orders_topic)
    async def enrich_orders(stream):
        """Join orders with customer profiles and emit enriched events."""
        async for order in stream:
            customer = customer_table.get(order.customer_id)
            if customer is None:
                customer = CustomerRecord(
                    customer_id=order.customer_id,
                    name="Unknown", email="unknown@example.com")
            enriched = EnrichedOrderRecord(
                order_id=order.order_id, customer_id=order.customer_id,
                customer_name=customer.name, customer_email=customer.email,
                customer_tier=customer.tier, items=order.items,
                total_amount=order.total_amount, currency=order.currency,
                is_high_value=order.total_amount > 500.0,
                region=order.region, enriched_at=time.time())
            await enriched_topic.send(key=order.order_id, value=enriched)
