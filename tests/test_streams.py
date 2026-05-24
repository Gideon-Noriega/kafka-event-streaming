"""Tests for stream processing."""
import pytest
from src.streams.state_machine import OrderStatus, VALID_TRANSITIONS


class TestOrderStateMachine:
    @pytest.mark.unit
    def test_placed_to_paid(self):
        assert OrderStatus.PAID in VALID_TRANSITIONS[OrderStatus.PLACED]

    @pytest.mark.unit
    def test_placed_to_cancelled(self):
        assert OrderStatus.CANCELLED in VALID_TRANSITIONS[OrderStatus.PLACED]

    @pytest.mark.unit
    def test_invalid_placed_to_shipped(self):
        assert OrderStatus.SHIPPED not in VALID_TRANSITIONS[OrderStatus.PLACED]

    @pytest.mark.unit
    def test_delivered_terminal(self):
        assert len(VALID_TRANSITIONS[OrderStatus.DELIVERED]) == 0

    @pytest.mark.unit
    def test_paid_to_fulfilled(self):
        assert OrderStatus.FULFILLED in VALID_TRANSITIONS[OrderStatus.PAID]

    @pytest.mark.unit
    def test_payment_failed_retry(self):
        assert OrderStatus.PLACED in VALID_TRANSITIONS[OrderStatus.PAYMENT_FAILED]

    @pytest.mark.unit
    def test_all_states_defined(self):
        for s in OrderStatus:
            assert s in VALID_TRANSITIONS
