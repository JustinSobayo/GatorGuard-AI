from backend.orchestration.crime_producer import normalize_crime_record, normalize_crime_records


def test_normalize_crime_record_maps_common_fields():
    record = {
        "incident_id": "abc-123",
        "incident_type": "Theft",
        "incident_date": "2026-04-29T22:15:00",
        "reported_date": "2026-04-30T01:00:00",
        "latitude": "29.6516",
        "longitude": "-82.3248",
        "address": "University Ave",
    }

    result = normalize_crime_record(record)

    assert result is not None
    assert result["id"] == "abc-123"
    assert result["incident_type"] == "Theft"
    assert result["offense_hour_of_day"] == "22"
    assert result["offense_day_of_week"] == "Wednesday"
    assert result["latitude"] == "29.6516"
    assert result["longitude"] == "-82.3248"


def test_normalize_crime_record_extracts_socrata_coordinates():
    record = {
        "case_number": "case-1",
        "narrative": "Burglary",
        "offense_date": "2026-04-30",
        "geocoded_column": {"coordinates": [-82.32, 29.65]},
    }

    result = normalize_crime_record(record)

    assert result is not None
    assert result["id"] == "case-1"
    assert result["latitude"] == "29.65"
    assert result["longitude"] == "-82.32"


def test_normalize_crime_records_skips_ungeocoded_rows():
    records = [
        {"incident_id": "missing-coords"},
        {"incident_id": "ok", "latitude": 29.65, "longitude": -82.32},
    ]

    assert len(normalize_crime_records(records)) == 1
