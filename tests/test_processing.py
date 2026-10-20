import pytest
from pydantic import ValidationError

from app.common.schemas import ShipmentIn
from app.database.store import process_event


def test_temperature_reading_inside_registered_band_is_accepted():
    result = process_event(
        {"kind": "temperature_reading", "key": "SHIP-1", "payload": {}},
        {"shipment_id": "SHIP-1", "sensor_id": "sensor-1", "temperature_c": 5.2, "in_range": True},
    )
    assert result["in_range"] is True
    assert result["assessment"] == "within_shipment_limits"
    assert result["alert"] is None


def test_temperature_reading_outside_registered_band_is_flagged():
    result = process_event(
        {"kind": "temperature_reading", "key": "SHIP-1", "payload": {}},
        {"shipment_id": "SHIP-1", "sensor_id": "sensor-1", "temperature_c": 9.1, "in_range": False},
    )
    assert result["in_range"] is False
    assert result["assessment"] == "outside_shipment_limits"
    assert result["alert"] == "temperature_excursion"


def test_shipment_limits_must_form_a_valid_band():
    with pytest.raises(ValidationError):
        ShipmentIn(shipment_id="SHIP-1", description="Clinic delivery", min_temp_c=8, max_temp_c=2)


def test_shipment_identifier_is_safe_for_route_and_partition_key():
    with pytest.raises(ValidationError):
        ShipmentIn(shipment_id="../other", description="Clinic delivery", min_temp_c=2, max_temp_c=8)
