from floorist.helpers import generate_name
from floorist.helpers import validate_floorplan_entry
from datetime import date

import pytest


def test_name_without_prefix():
    bucket_name = "my_bucket"
    actual_name = generate_name("my_bucket")
    name = date.today().strftime('year_created=%Y/month_created=%-m/day_created=%-d')
    expected_name = f"s3://{bucket_name}/{name}"

    assert actual_name == expected_name


def test_name_with_prefix():
    bucket_name = "my_bucket"
    prefix = "some-prefix"
    actual_name = generate_name(bucket_name, prefix)
    name = date.today().strftime('year_created=%Y/month_created=%-m/day_created=%-d')
    expected_name = f"s3://{bucket_name}/{prefix}/{name}"

    assert actual_name == expected_name

@pytest.mark.parametrize("query,prefix", [(None, "prefix"), (None, None), ("query", None)])
def test_validate_floorplan_entry_captures_invalid_data(query,prefix):
    with pytest.raises(ValueError) as excinfo:
        validate_floorplan_entry(query,prefix)

    if (not prefix and not query) or not query:
        assert "Query cannot be empty!" in str(excinfo.value)
    elif not prefix:
        assert "Prefix cannot be empty" in str(excinfo.value)

def test_validate_floorplan_entry_checks_valid_data():
    assert validate_floorplan_entry("query", "prefix")
