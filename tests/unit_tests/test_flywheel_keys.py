import flywheel
import pytest
import datetime
import utils

import transfer_log


def test_flywheel_key():
    config = transfer_log.Config({
        'query': [{'session.label': 'Label'}, {'project.label': 'Project'}],
        'join': 'session'
    })
    row = {'session.label': 'ses-1', 'project.label': 'My Project'}

    expected_key = 'ses-1', 'My Project'
    assert expected_key == transfer_log.key_from_flywheel(row, config)


def test_flywheel_key_timestamp():
    config = transfer_log.Config({
        'query': [{'session.timestamp': 'Date', 'timeformat': '%b %d, %Y'}],
        'join': 'session'
    })
    row = {
        'session.label': 'ses-1',
        'session.timestamp': str(datetime.datetime(1970, 1, 1))
    }

    expected_key = 'Jan 01, 1970',
    assert expected_key == transfer_log.key_from_flywheel(row, config)


def test_flywheel_key_missing_key():
    config = transfer_log.Config({
        'query': [{'session.age': 'Age'}],
        'join': 'session'
    })
    row = {
        'session.label': 'ses-1',
    }

    expected_key = None,
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

    expected_key = 'ses-1', 'My Project'
    assert expected_key == transfer_log.key_from_metadata(row, config)


def test_metadata_key_pattern():
    config = transfer_log.Config({
        'query': [{'session.timestamp': 'Date', 'pattern': '[^-]+$'}],
        'join': 'session'
    })
    row = {'Date': 'MR - Jan 01, 1970'}

    expected_key = 'Jan 01, 1970',
    assert expected_key == transfer_log.key_from_metadata(row, config)


def test_metadata_missing_pattern_key():
    config = transfer_log.Config({
        'query': [{'session.timestamp': 'Date', 'pattern': '[^-]+$'}],
        'join': 'session'
    })
    row = {'Label': 'ses-1'}

    expected_key = None,
    assert expected_key == transfer_log.key_from_metadata(row, config)
