"""Tests for consumer implementations."""
import json, time
from unittest.mock import MagicMock, patch
import pytest
from src.consumers.dlq_handler import DeadLetterQueueHandler


class TestDeadLetterQueueHandler:
    @pytest.mark.unit
    def test_permanent_error_no_retry(self):
        count = 0
        def fail(msg):
            nonlocal count; count += 1
            raise ValueError("schema validation")
        with patch("src.consumers.dlq_handler.Consumer"), \
             patch("src.consumers.dlq_handler.Producer"):
            h = DeadLetterQueueHandler("test", fail)
            msg = MagicMock()
            msg.value.return_value = b"x"
            assert h._process_with_retry(msg) is False
            assert count == 1

    @pytest.mark.unit
    def test_transient_retries(self):
        count = 0
        def flaky(msg):
            nonlocal count; count += 1
            if count < 3: raise TimeoutError()
        with patch("src.consumers.dlq_handler.Consumer"), \
             patch("src.consumers.dlq_handler.Producer"), \
             patch("time.sleep"):
            h = DeadLetterQueueHandler("test", flaky)
            msg = MagicMock()
            assert h._process_with_retry(msg) is True
            assert count == 3

    @pytest.mark.unit
    def test_error_classification(self):
        with patch("src.consumers.dlq_handler.Consumer"), \
             patch("src.consumers.dlq_handler.Producer"):
            h = DeadLetterQueueHandler("test", lambda m: None)
            assert h._classify_error(TimeoutError()) == "timeout"
            assert h._classify_error(json.JSONDecodeError("","",0)) == "deserialization_error"
            assert h._classify_error(RuntimeError()) == "unknown"
