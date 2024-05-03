from orch.utils.storage import (
    get_utc_timestamp,
    make_workspace_dirs,
    extract_key_from_path,
    extract_keys_from_path,
)
import os
import datetime


def test_extract_key_from_path():
    assert (
        extract_key_from_path("my_folder/year=2022/month=01/day=01", "year") == "2022"
    )
    assert extract_key_from_path("my_folder/year=2022/month=01/day=01", "month") == "01"
    assert extract_key_from_path("my_folder/year=2022/month=01/day=01", "day") == "01"
    assert extract_key_from_path("my_folder/year=2022/month=01/day=01", "hour") == None
    assert (
        extract_key_from_path("my_folder/year=2022/month=01/day=01", "invalid_key")
        == None
    )
    assert extract_key_from_path("my_folder/year=2022/month=01/day=01", "") == None
    assert extract_key_from_path("", "year") == None


def test_extract_keys_from_path():
    assert extract_keys_from_path("my_folder/year=2022/month=01/day=01") == {
        "year": "2022",
        "month": "01",
        "day": "01",
    }
    assert extract_keys_from_path("my_folder/year=2022/month=01/day=01/hour=12") == {
        "year": "2022",
        "month": "01",
        "day": "01",
        "hour": "12",
    }
    assert extract_keys_from_path("my_folder") == {}
    assert extract_keys_from_path("") == {}
    assert extract_keys_from_path("my_folder////invalid") == {}


def test_get_utc_timestamp():
    # Call the function
    current_timestamp = get_utc_timestamp()

    # Assert that the timestamp is in the correct format
    expected_timestamp_format = "%Y-%m-%dT%H:%M:%SZ"
    parsed_timestamp = datetime.datetime.strptime(
        current_timestamp, expected_timestamp_format
    )
    assert current_timestamp == parsed_timestamp.strftime(expected_timestamp_format)


def test_make_worspace_dirs():

    # Call the function
    raw_dir, stg_dir = make_workspace_dirs()

    # Assert that the raw and staged directories are created
    assert os.path.exists(raw_dir)
    assert os.path.exists(stg_dir)
    # Assert that the raw and staged directories are different
    assert raw_dir != stg_dir
