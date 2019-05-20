import pytest
import transfer_log


def test_config_match():
    config = transfer_log.Config({
        'query': [{'session.label': 'Label'}, {'project.label': 'Project'}],
        'join': 'session'
    })
    row = {'Label': 'ses-01', 'Project': 'My Project'}
    assert transfer_log.check_config_and_log_match(config, row)


def test_config_no_match():
    config = transfer_log.Config({
        'query': [{'session.label': 'Label'}, {'project.label': 'Project'}],
        'join': 'session'
    })
    row = {'Label': 'ses-01'}
    assert not transfer_log.check_config_and_log_match(config, row)

