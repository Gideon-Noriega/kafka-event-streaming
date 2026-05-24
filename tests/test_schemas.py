"""Tests for Avro schema compatibility."""
import json
from pathlib import Path
import pytest

SCHEMAS = Path(__file__).parent.parent / "src" / "schemas"


class TestSchemaEvolution:
    @pytest.mark.unit
    def test_v1_valid(self):
        s = json.loads((SCHEMAS / "order_placed_v1.avsc").read_text())
        assert s["type"] == "record"
        assert s["name"] == "OrderPlaced"
        assert len(s["fields"]) == 9

    @pytest.mark.unit
    def test_v2_valid(self):
        s = json.loads((SCHEMAS / "order_placed_v2.avsc").read_text())
        assert len(s["fields"]) == 12

    @pytest.mark.unit
    def test_v2_new_fields_have_defaults(self):
        v1 = json.loads((SCHEMAS / "order_placed_v1.avsc").read_text())
        v2 = json.loads((SCHEMAS / "order_placed_v2.avsc").read_text())
        v1_names = {f["name"] for f in v1["fields"]}
        new = [f for f in v2["fields"] if f["name"] not in v1_names]
        for field in new:
            assert "default" in field, f"{field[chr(110)+chr(97)+chr(109)+chr(101)]} needs default"

    @pytest.mark.unit
    def test_v1_fields_unchanged(self):
        v1 = json.loads((SCHEMAS / "order_placed_v1.avsc").read_text())
        v2 = json.loads((SCHEMAS / "order_placed_v2.avsc").read_text())
        v1f = {f["name"]: f["type"] for f in v1["fields"]}
        v2f = {f["name"]: f["type"] for f in v2["fields"]}
        for name, t in v1f.items():
            assert name in v2f
            assert v2f[name] == t

    @pytest.mark.unit
    def test_payment_schema(self):
        s = json.loads((SCHEMAS / "payment_processed.avsc").read_text())
        assert s["name"] == "PaymentProcessed"
        names = {f["name"] for f in s["fields"]}
        assert "payment_id" in names and "order_id" in names
