"""Domain models for the order processing system."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class OrderStatus(str, Enum):
    """Order lifecycle states."""

    PLACED = "placed"
    PAID = "paid"
    PAYMENT_FAILED = "payment_failed"
    FULFILLED = "fulfilled"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class PaymentMethod(str, Enum):
    """Supported payment methods."""

    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    PAYPAL = "paypal"
    BANK_TRANSFER = "bank_transfer"
    CRYPTO = "crypto"


class Region(str, Enum):
    """Customer regions for partitioning."""

    US_EAST = "us-east"
    US_WEST = "us-west"
    EU_WEST = "eu-west"
    EU_CENTRAL = "eu-central"
    APAC = "apac"


class OrderItem(BaseModel):
    """Individual item in an order."""

    product_id: str
    product_name: str
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(gt=0)

    @property
    def total_price(self) -> Decimal:
        return self.unit_price * self.quantity


class Customer(BaseModel):
    """Customer profile for KTable enrichment."""

    customer_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    email: str
    name: str
    region: Region
    tier: str = "standard"  # standard, premium, enterprise
    lifetime_orders: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)


class OrderPlaced(BaseModel):
    """Event: A new order has been placed."""

    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str
    items: list[OrderItem]
    total_amount: Decimal
    currency: str = "USD"
    payment_method: PaymentMethod
    shipping_address: str
    region: Region
    placed_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, str] = Field(default_factory=dict)


class OrderEnriched(BaseModel):
    """Event: Order enriched with customer data."""

    order_id: str
    customer_id: str
    customer_name: str
    customer_email: str
    customer_tier: str
    customer_region: Region
    items: list[OrderItem]
    total_amount: Decimal
    currency: str
    payment_method: PaymentMethod
    shipping_address: str
    is_high_value: bool
    placed_at: datetime
    enriched_at: datetime = Field(default_factory=datetime.utcnow)


class PaymentProcessed(BaseModel):
    """Event: Payment has been processed."""

    payment_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str
    customer_id: str
    amount: Decimal
    currency: str = "USD"
    payment_method: PaymentMethod
    status: str  # "success" or "failed"
    failure_reason: Optional[str] = None
    transaction_ref: str = Field(default_factory=lambda: f"txn-{uuid.uuid4().hex[:12]}")
    processed_at: datetime = Field(default_factory=datetime.utcnow)


class ShipmentCreated(BaseModel):
    """Event: Shipment has been created."""

    shipment_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str
    customer_id: str
    carrier: str
    tracking_number: str
    estimated_delivery: datetime
    items_count: int
    weight_kg: float
    created_at: datetime = Field(default_factory=datetime.utcnow)


class FraudAlert(BaseModel):
    """Event: Potential fraud detected."""

    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id: str
    order_count_in_window: int
    total_amount_in_window: Decimal
    window_start: datetime
    window_end: datetime
    risk_score: float = Field(ge=0.0, le=1.0)
    reason: str
    triggered_at: datetime = Field(default_factory=datetime.utcnow)


class OrderStateChange(BaseModel):
    """Event: Order status transition."""

    order_id: str
    customer_id: str
    previous_status: Optional[OrderStatus]
    new_status: OrderStatus
    changed_at: datetime = Field(default_factory=datetime.utcnow)
    reason: Optional[str] = None


class DeadLetterRecord(BaseModel):
    """Record sent to the dead letter queue."""

    original_topic: str
    original_partition: int
    original_offset: int
    original_key: Optional[str]
    original_value: bytes
    error_message: str
    error_type: str
    retry_count: int
    first_failure_at: datetime
    last_failure_at: datetime = Field(default_factory=datetime.utcnow)
    stack_trace: Optional[str] = None
