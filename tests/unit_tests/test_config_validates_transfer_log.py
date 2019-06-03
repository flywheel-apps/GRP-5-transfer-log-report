import pytest
import transfer_log


def test_config_validates_transfer_log():
    config = transfer_log.Config({
        'query': [{'session.label': 'Label'}, {'project.label': 'Project'}],
        'join': 'session'
    })
    rows = [{'Label': 'ses-01', 'Project': 'My Project'}]
    errors = transfer_log.check_config_and_log_match(config, rows)
    assert not errors


def test_transfer_log_missing_column():
    config = transfer_log.Config({
        'query': [{'session.label': 'Label'}, {'project.label': 'Project'}],
        'join': 'session'
    })
    rows = [{'Label': 'ses-01'}]
    errors = transfer_log.check_config_and_log_match(config, rows)
    assert len(errors) == 1
    assert errors[0]['column'] == 'Project'
    assert errors[0]['error'] == 'Transfer log missing column Project'


def test_transfer_log_invalid_value():
    config = transfer_log.Config({
        'query': [
            {'session.label': 'Label', 'validate': '^[0-9]+$'},
            {'project.label': 'Project'}
        ],
        'join': 'session'
    })
    rows = [{'Label': 'ses-01', 'Project': 'My Project'}]
    errors = transfer_log.check_config_and_log_match(config, rows)
    print(errors)
    assert len(errors) == 1
    assert errors[0]['row'] == 1
    assert errors[0]['column'] == 'Label'


