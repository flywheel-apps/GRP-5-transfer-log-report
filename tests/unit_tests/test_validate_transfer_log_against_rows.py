import datetime
import pytest
import mock

import transfer_log

DATE = datetime.datetime.now()


def test_validate_project():
    flywheel_table = {('a','b','c'): 'ses-1', ('a','B','C'): 'ses-2'}
    metadata = {('a','b','c'): 'row1', ('a','B','C'): 'row2'}
    config = transfer_log.Config({
        'query': [{'a':'a'}, {'b':'b'}, {'c':'c'}],
        'join': 'session'
    })

    missing_containers, found_containers, unexpected_containers =  \
        transfer_log.validate_flywheel_against_metadata(flywheel_table, metadata,
                                                        config)

    assert len(missing_containers) == 0
    assert len(found_containers) == 2
    print(found_containers)
    print(type(found_containers))
    assert found_containers[0] == 'ses-1'
    assert len(unexpected_containers) == 0


def test_validate_project_with_missing_sessions():
    flywheel_table = {('a','b','c'): 'ses-1'}
    metadata = {('a','b','c'): 'row1', ('a','B','C'): 'row2'}
    config = transfer_log.Config({
        'query': [{'a':'a'}, {'b':'b'}, {'c':'c'}],
        'join': 'session'
    })

    missing_containers, found_containers, unexpected_containers =  \
        transfer_log.validate_flywheel_against_metadata(flywheel_table, metadata,
                                                        config)


    assert len(missing_containers) == 1
    assert missing_containers[('a', 'B', 'C')] == 'row2'
    assert len(found_containers) == 1
    assert found_containers[0] == 'ses-1'
    assert len(unexpected_containers) == 0


def test_validate_project_with_unexpected_sessions():
    flywheel_table = {('a','b','c'): 'ses-1', ('a','B','C'): 'ses-2'}
    metadata = {('a','b','c'): 'row1'}
    config = transfer_log.Config({
        'query': [{'a':'a'}, {'b':'b'}, {'c':'c'}],
        'join': 'session'
    })

    missing_containers, found_containers, unexpected_containers =  \
        transfer_log.validate_flywheel_against_metadata(flywheel_table, metadata,
                                                        config)


    assert len(missing_containers) == 0
    assert len(found_containers) == 1
    assert found_containers[0] == 'ses-1'
    assert len(unexpected_containers) == 1
    assert unexpected_containers[('a', 'B', 'C')] == 'ses-2'


def test_create_missing_error():
    key = ('a', 'b', 'c')
    container_type = 'session'
    expected_error_msg = 'session (\'a\', \'b\', \'c\') missing from flywheel'

    assert expected_error_msg == transfer_log.create_missing_error(key, container_type).get('error')


def test_create_missing_error():
    container = mock.MagicMock(id='id', label='label', container_type='session')
    expected_error = {
        'error': 'session in flywheel not present in transfer log',
        'path': 'Path/to/container',
        'type': 'session',
        'resolved': False,
        'label': 'label',
        '_id': 'id'
    }
    with mock.patch('transfer_log.utils.get_resolver_path',
                    return_value='Path/to/container'):
        assert expected_error == transfer_log.create_unexpected_error(container, None)


def test_metadata_day_month_missing_zero_pad():
    flywheel_table = {('266099', '1129', 'screening', '08/01/2014', '5dcd6836c01312003e6512bc'): 'ses-1'}

    config = transfer_log.Config(
        {'query': [
            {'subject.info.ClinicalTrialSiteID': 'SITE'},
            {'subject.label': 'SUBJECT'}, {'session.label': 'VISIT'},
            {'session.timestamp': 'SCAN DATE', 'timeformat': '%m/%d/%Y'}],
         'join': 'session'}
    )
    metadata = {transfer_log.key_from_metadata(
        {'SITE': '266099',
         'SUBJECT': '1129',
         'VISIT': 'screening',
         'SCAN DATE': '8/1/2014'},
        config
    ): 2}
    missing_containers, found_containers, unexpected_containers = \
        transfer_log.validate_flywheel_against_metadata(flywheel_table, metadata,
                                                        config)
    assert len(missing_containers) == 0
    assert len(found_containers) == 1
    assert found_containers[0] == 'ses-1'
    assert len(unexpected_containers) == 0