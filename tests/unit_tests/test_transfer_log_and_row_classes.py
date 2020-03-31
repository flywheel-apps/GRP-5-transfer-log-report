import datetime
from pathlib import Path

import transfer_log

DATA_ROOT = Path(__file__).parent / 'data'


def test_transfer_log_class():
    metadata_path = DATA_ROOT / 'test-transfer-log.xlsx'
    config_path = DATA_ROOT / 'test-transfer-log-template.yml'
    config = transfer_log.load_config_file(config_path)
    test_transfer_log = transfer_log.TransferLog(client=None, config=config, transfer_log_path=metadata_path,
                                                 project_id=None, case_insensitive=False)
    expected_error_dict = {'row_or_id': None, 'subject.label': None, 'session.timestamp': None, 'session.label': None,
                           'file.modality': None, 'error': None, 'path': None, 'type': None, 'resolved': False,
                           'label': None}
    assert expected_error_dict == test_transfer_log.error_dict

    test_transfer_log.load_metadata_table()
    test_transfer_log.match_fw_to_tl()
    error_list = test_transfer_log.get_errors(dry_run=True)
    assert len(error_list) == len(test_transfer_log.metadata_table)


def test_metadata_row():
    config = transfer_log.Config(
        {'query': [
            {'subject.info.ClinicalTrialSiteID': 'SITE'},
            {'subject.label': 'SUBJECT'},
            {'session.label': 'VISIT'},
            {'session.timestamp': 'SCAN DATE', 'timeformat': '%m/%d/%Y'}],
            'join': 'session'}
    )
    test_meta_dict = {'SITE': '266099', 'SUBJECT': '1129', 'VISIT': 'screening', 'SCAN DATE': '8/1/2014'}
    test_meta_row = transfer_log.MetadataRow(config=config, row_dict=test_meta_dict, index=2, case_insensitive=False)
    expected_match_dict = {'subject.info.ClinicalTrialSiteID': '266099', 'subject.label': '1129',
                           'session.label': 'screening', 'session.timestamp': '08/01/2014'}
    assert test_meta_row.match_dict == expected_match_dict
    template_dict = transfer_log.get_template_error_dict(config)
    expected_error_dict = template_dict.copy()
    expected_error_dict['row_or_id'] = 4
    expected_error_dict['label'] = None
    expected_error_dict['type'] = 'session'
    for key in expected_error_dict.keys():
        if test_meta_row.match_dict.get(key):
            expected_error_dict[key] = test_meta_row.match_dict[key]
    expected_error_message = 'row 4 missing from flywheel'
    expected_error_dict['error'] = expected_error_message
    assert test_meta_row.get_error_message() == expected_error_message
    assert test_meta_row.get_error_dict(template_dict, client=None, dry_run=True) == expected_error_dict
    assert test_meta_row.spreadsheet_index == 4


def test_flywheel_row():
    config = transfer_log.Config(
        {'query': [
            {'subject.info.ClinicalTrialSiteID': 'SITE'},
            {'subject.label': 'SUBJECT'},
            {'session.label': 'VISIT'},
            {'session.timestamp': 'SCAN DATE', 'timeformat': '%m/%d/%Y'}],
            'join': 'session'}
    )
    test_fw_dict = {'subject.info.ClinicalTrialSiteID': '266099', 'subject.label': '1129',
                    'session.label': 'screening',
                    'session.timestamp': str(datetime.datetime(2014, 8, 1, tzinfo=datetime.timezone.utc))}

    test_fw_index = 'test_id'
    test_fw_row = transfer_log.FlywheelRow(config=config, row_dict=test_fw_dict.copy(), index=test_fw_index,
                                           case_insensitive=False)
    test_fw_dict['session.timestamp'] = '08/01/2014'
    assert test_fw_row.spreadsheet_index == 'test_id'
    assert test_fw_row.match_dict == test_fw_dict
    template_dict = transfer_log.get_template_error_dict(config)
    expected_error_dict = template_dict.copy()
    expected_error_dict['row_or_id'] = 'test_id'
    expected_error_dict['label'] = 'test_label'
    expected_error_dict['type'] = 'session'
    for key in expected_error_dict.keys():
        if test_fw_row.match_dict.get(key):
            expected_error_dict[key] = test_fw_row.match_dict[key]
    expected_error_message = 'session in flywheel contains no files'

    assert test_fw_row.get_error_message({'label': 'test_label', 'id': 'test_id'}) == expected_error_message

    assert test_fw_row.get_error_message(
        {'label': 'test_label', 'id': 'test_id', 'files': [{'name': 'test_file'}]}
    ) == 'session in flywheel not present in transfer log'
