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

def test_config_mappings():
    config = transfer_log.Config({
        'query': [{'session.label': 'Label'}, {'project.label': 'Project'}],
        'join': 'session',
        'mappings': {
            'Week 4': ['w04', 'wk4', 'Week_4']
        }
    })

    assert config.mappings == {
        'w04': 'Week 4',
        'wk4': 'Week 4',
        'Week_4': 'Week 4'
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

    expected_key = 'Week 4', 'My Project'
    assert expected_key == transfer_log.key_from_flywheel(row, config)


def test_flywheel_key_with_flywheel_items():
    config = transfer_log.Config({
        'query': [{'session.label': False}, {'project.label': 'Project'}],
        'join': 'session',
    })
    row = {'session.label': 'ses-1', 'project.label': 'My Project'}

    expected_key = 'ses-1', 'My Project'
    assert expected_key == transfer_log.key_from_flywheel(row, config)


def test_metadata_key_with_flywheel_items():
    config = transfer_log.Config({
        'query': [{'session.label': False}, {'project.label': 'Label'}, {'acquisition.id': False}],
        'join': 'acquisition'
    })
    row = {'session.label': 'ses-01', 'Label': 'Project Label'}

    expected_key = None, 'Project Label', None
    assert expected_key == transfer_log.key_from_metadata(row, config)


def test_formatting_flywheel_key():
    config = transfer_log.Config({
        'query': [{'session.label': False}, {'project.label': 'Label'}],
        'join': 'acquisition'
    })
    flywheel_key = 'ses-01', 'Project Label'

    expected_key = None, 'Project Label'
    assert expected_key == transfer_log.format_flywheel_key(flywheel_key, config)

