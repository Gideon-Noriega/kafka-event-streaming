"""Test fixtures."""
import uuid
from decimal import Decimal
import pytest
from src.models import Customer, OrderItem, OrderPlaced, PaymentMethod, PaymentProcessed, Region


@pytest.fixture
def sample_order() -> OrderPlaced:
    return OrderPlaced(
        order_id="order-test-001", customer_id="cust-test-001",
        items=[OrderItem(product_id="LAPTOP-001", product_name="MacBook Pro",
            quantity=1, unit_price=Decimal("2499.99"))],
        total_amount=Decimal("2499.99"),
        payment_method=PaymentMethod.CREDIT_CARD,
        shipping_address="123 Test St", region=Region.US_EAST)


@pytest.fixture
def sample_customer() -> Customer:
    return Customer(customer_id="cust-test-001", email="test@example.com",
        name="Test Customer", region=Region.US_EAST, tier="premium")


@pytest.fixture
def high_value_order() -> OrderPlaced:
    return OrderPlaced(
        order_id=f"order-{uuid.uuid4().hex[:8]}", customer_id="cust-whale",
        items=[OrderItem(product_id="LUX-001", product_name="Diamond Watch",
            quantity=3, unit_price=Decimal("9999.99"))],
        total_amount=Decimal("29999.97"),
        payment_method=PaymentMethod.CREDIT_CARD,
        shipping_address="1 Rich Ave", region=Region.US_WEST)


@pytest.fixture
def order_batch() -> list[OrderPlaced]:
    return [OrderPlaced(
        order_id=f"order-batch-{i:04d}", customer_id=f"cust-{i%10:04d}",
        items=[OrderItem(product_id=f"PROD-{i%5:03d}", product_name=f"Product {i%5}",
            quantity=1, unit_price=Decimal("49.99"))],
        total_amount=Decimal("49.99"),
        payment_method=PaymentMethod.CREDIT_CARD,
        shipping_address="456 Batch St",
        region=Region(["us-east","us-west","eu-west","eu-central","apac"][i%5]))
    for i in range(100)]
