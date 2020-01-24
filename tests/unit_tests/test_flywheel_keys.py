import flywheel
import pytest
from unittest.mock import MagicMock
import datetime
import urllib3
import utils
import json

import transfer_log


def test_flywheel_key():
    config = transfer_log.Config({
        'query': [{'session.label': 'Label'}, {'project.label': 'Project'}],
        'join': 'session'
    })
    row = {'session.label': 'ses-1', 'project.label': 'My Project'}

    expected_key = ['ses-1'], ['My Project'], None
    assert expected_key == transfer_log.key_from_flywheel(row, config)


def test_flywheel_key_timestamp():
    config = transfer_log.Config({
        'query': [{'session.timestamp': 'Date', 'timeformat': '%b %d, %Y'}],
        'join': 'session'
    })
    row = {
        'session.label': 'ses-1',
        'session.timestamp': str(datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc))
    }

    expected_key = ['Jan 01, 1970'], None
    assert expected_key == transfer_log.key_from_flywheel(row, config)


def test_flywheel_key_timestamp_timezone():
    config = transfer_log.Config({
        'query': [{
            'session.timestamp': 'Date',
            'timeformat': '%b %d, %Y',
            'timezone': 'America/Chicago'
        }]
    })
    row = {
        'session.label': 'ses-1',
        'session.timestamp': str(datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc))
    }
    expected_key = ['Dec 31, 1969'], None
    assert expected_key == transfer_log.key_from_flywheel(row, config)


def test_flywheel_key_timezone():
    config = transfer_log.Config({
        'query': [{
            'session.timestamp': 'Date',
            'timeformat': '%b %d, %Y',
            'timezone': 'Not_a_timezone'
        }]
    })
    row = {
        'session.label': 'ses-1',
        'session.timestamp': str(datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc))
    }
    expected_key = ['Jan 01, 1970'], None
    assert expected_key == transfer_log.key_from_flywheel(row, config)


def test_flywheel_key_missing_key():
    config = transfer_log.Config({
        'query': [{'session.age': 'Age'}],
        'join': 'session'
    })
    row = {
        'session.label': 'ses-1',
    }

    expected_key = None, None
    assert expected_key == transfer_log.key_from_flywheel(row, config)


def test_flywheel_key_invalid_timestamp():
    config = transfer_log.Config({
        'query': [{'session.timestamp': 'Date', 'timeformat': '%b %d, %Y'}],
        'join': 'session'
    })
    row = {
        'session.label': 'ses-1',
        'session.timestamp': 'Not a timestamp'
    }

    with pytest.raises(ValueError) as ve:
        transfer_log.key_from_flywheel(row, config)

    assert 'session.timestamp=Not a timestamp' in str(ve.value)


def test_metadata_key():
    config = transfer_log.Config({
        'query': [{'session.label': 'Label'}, {'project.label': 'Project'}],
        'join': 'session'
    })
    row = {'Label': 'ses-1', 'Project': 'My Project'}

    expected_key = ['ses-1'], ['My Project'], None
    assert expected_key == transfer_log.key_from_metadata(row, config)


def test_metadata_key_pattern():
    config = transfer_log.Config({
        'query': [{'session.timestamp': 'Date', 'pattern': '[^-]+$'}],
        'join': 'session'
    })
    row = {'Date': 'MR - Jan 01, 1970'}

    expected_key = ['Jan 01, 1970'], None
    assert expected_key == transfer_log.key_from_metadata(row, config)


def test_metadata_missing_pattern_key():
    config = transfer_log.Config({
        'query': [{'session.timestamp': 'Date', 'pattern': '[^-]+$'}],
        'join': 'session'
    })
    row = {'Label': 'ses-1'}

    expected_key = None, None
    assert expected_key == transfer_log.key_from_metadata(row, config)


def test_config_mappings():
    config = transfer_log.Config({
        'query': [{'session.label': 'Label'}, {'project.label': 'Project'}],
        'join': 'session',
        'mappings': {
            'Week 4': ['w04', 'wk4', 'Week_4']
        }
    })

    assert config.mappings == {
        'w04': ['Week 4'],
        'wk4': ['Week 4'],
        'Week_4': ['Week 4']
    }


def test_config_mappings_with_duplicates():
    config = transfer_log.Config({
        'query': [{'session.label': 'Label'}, {'project.label': 'Project'}],
        'join': 'session',
        'mappings': {
            'Week 4': ['w04', 'wk4', 'Week_4'],
            'Week_4': ['w04', 'wk4']
        }
    })

    assert config.mappings == {
        'w04': ['Week 4', 'Week_4'],
        'wk4': ['Week 4', 'Week_4'],
        'Week_4': ['Week 4']
    }


def test_flywheel_mappings():
    config = transfer_log.Config({
        'query': [{'session.label': 'Label'}, {'project.label': 'Project'}],
        'join': 'session',
        'mappings': {
            'Week 4': ['w04', 'wk4', 'Week_4']
        }
    })
    row = {'session.label': 'w04', 'project.label': 'My Project'}

    expected_key = ['Week 4'], ['My Project'], None
    assert expected_key == transfer_log.key_from_flywheel(row, config)


def test_flywheel_key_with_flywheel_items():
    config = transfer_log.Config({
        'query': [{'session.label': False}, {'project.label': 'Project'}],
        'join': 'session',
    })
    row = {'session.label': 'ses-1', 'project.label': 'My Project'}

    expected_key = ['ses-1'], ['My Project'], None
    assert expected_key == transfer_log.key_from_flywheel(row, config)


def test_metadata_key_with_flywheel_items():
    config = transfer_log.Config({
        'query': [{'session.label': False}, {'project.label': 'Label'}, {'acquisition.id': False}],
        'join': 'acquisition'
    })
    row = {'session.label': 'ses-01', 'Label': 'Project Label'}

    expected_key = None, ['Project Label'], None
    assert expected_key == transfer_log.key_from_metadata(row, config)


def test_formatting_flywheel_key():
    config = transfer_log.Config({
        'query': [{'session.label': False}, {'project.label': 'Label'}],
        'join': 'acquisition'
    })
    flywheel_key = 'ses-01', 'Project Label'

    expected_key = None, 'Project Label'
    assert expected_key == transfer_log.format_flywheel_key(flywheel_key, config)


def test_flywheel_key_with_flywheel_items_and_case():
    config = transfer_log.Config({
        'query': [{'session.label': False}, {'project.label': 'Project'}],
        'join': 'session',
    })
    row = {'session.label': 'ses-1', 'project.label': 'My Project'}

    expected_key = ['ses-1'], ['my project'], None
    assert expected_key == transfer_log.key_from_flywheel(row, config, True)


def test_metadata_key_with_flywheel_items_and_case():
    config = transfer_log.Config({
        'query': [{'session.label': False}, {'project.label': 'Label'}, {'acquisition.id': False}],
        'join': 'acquisition'
    })
    row = {'session.label': 'ses-01', 'Label': 'Project Label'}

    expected_key = None, ['project label'], None
    assert expected_key == transfer_log.key_from_metadata(row, config, True)


def test_get_clean_dtypes():
    client = MagicMock()
    resp_data =[{"acquisition.id": "5cf7ec6bd9a631002dfddefd", "file.info.SeriesNumber": 10},
                {"acquisition.id": "5cf7ec6bd9a631002dfddefd", "file.info.SeriesNumber": None}]
    resp = urllib3.response.HTTPResponse(body=bytes(json.dumps(resp_data), 'utf-8'))
    client.read_view_data = MagicMock(return_value=resp)

    df_dtypes = transfer_log.get_clean_dtypes(client, None, '', ignore_cols=None)

    assert df_dtypes['file.info.SeriesNumber'] == 'Int64'


def test_get_clean_dtypes_log_exception_if_something_unexpected_happen(caplog):
    client = MagicMock()
    resp = urllib3.response.HTTPResponse(body=json.dumps(None))
    client.read_view_data = MagicMock(return_value=resp)

    _ = transfer_log.get_clean_dtypes(client, None, '', ignore_cols=None)
    assert 'An exception raises when trying to clean dtypes' in caplog.messages[0]
