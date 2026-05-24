"""Order state machine - stateful transformation."""
from __future__ import annotations
import time
from enum import Enum
from typing import Optional
import faust
from src.streams.app import app


class OrderStatus(str, Enum):
    PLACED = "placed"
    PAID = "paid"
    PAYMENT_FAILED = "payment_failed"
    FULFILLED = "fulfilled"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


VALID_TRANSITIONS = {
    OrderStatus.PLACED: {OrderStatus.PAID, OrderStatus.PAYMENT_FAILED, OrderStatus.CANCELLED},
    OrderStatus.PAID: {OrderStatus.FULFILLED, OrderStatus.CANCELLED},
    OrderStatus.PAYMENT_FAILED: {OrderStatus.PLACED, OrderStatus.CANCELLED},
    OrderStatus.FULFILLED: {OrderStatus.SHIPPED},
    OrderStatus.SHIPPED: {OrderStatus.DELIVERED},
    OrderStatus.DELIVERED: set(),
    OrderStatus.CANCELLED: set(),
}


class StateChangeEvent(faust.Record):
    order_id: str
    customer_id: str
    requested_status: str
    reason: Optional[str] = None


class OrderState(faust.Record):
    order_id: str
    customer_id: str
    current_status: str = "placed"
    history: list = None

    def __post_init__(self):
        if self.history is None:
            self.history = []


class StateChangeResult(faust.Record):
    order_id: str
    customer_id: str
    previous_status: str
    new_status: str
    accepted: bool
    reason: Optional[str] = None


state_input = app.topic("orders.state_change_requests", value_type=StateChangeEvent)
state_output = app.topic("orders.state_changes", value_type=StateChangeResult)
order_states = app.Table("order-states", default=OrderState, partitions=12)


class OrderStateMachine:
    """Manages order lifecycle transitions with validation.

    State diagram:
        placed --> paid --> fulfilled --> shipped --> delivered
           |         |
           v         v
        cancelled  cancelled
           ^
           |
        payment_failed --> placed (retry)
    """

    @staticmethod
    @app.agent(state_input)
    async def process_state_changes(stream):
        async for event in stream:
            state = order_states.get(event.order_id)
            if state is None:
                state = OrderState(order_id=event.order_id,
                    customer_id=event.customer_id)
            current = OrderStatus(state.current_status)
            requested = OrderStatus(event.requested_status)
            valid_next = VALID_TRANSITIONS.get(current, set())

            if requested in valid_next:
                prev = state.current_status
                state.current_status = requested.value
                state.history.append({"from": prev, "to": requested.value, "at": time.time()})
                order_states[event.order_id] = state
                result = StateChangeResult(order_id=event.order_id,
                    customer_id=event.customer_id, previous_status=prev,
                    new_status=requested.value, accepted=True)
            else:
                result = StateChangeResult(order_id=event.order_id,
                    customer_id=event.customer_id, previous_status=state.current_status,
                    new_status=event.requested_status, accepted=False,
                    reason=f"Invalid: {current.value} -> {event.requested_status}")
            await state_output.send(key=event.order_id, value=result)
