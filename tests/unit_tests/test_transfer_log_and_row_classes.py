import datetime
from pathlib import Path

import pandas as pd
import flywheel
import transfer_log

DATA_ROOT = Path(__file__).parent / 'data'


def test_transfer_log_class():
    metadata_path = DATA_ROOT / 'test-transfer-log.xlsx'
    config_path = DATA_ROOT / 'test-transfer-log-template.yml'
    mock_view_path = DATA_ROOT / 'test-fw-view.csv'
    config = transfer_log.load_config_file(config_path)
    test_transfer_log = transfer_log.TransferLog(client=None, config=config, transfer_log_path=metadata_path,
                                                 project_id=None, case_insensitive=True, match_containers_once=True)
    expected_error_dict = {'row_or_id': None, 'subject.label': None, 'session.timestamp': None, 'session.label': None,
                           'file.modality': None, 'error': None, 'path': None, 'type': None, 'resolved': False,
                           'label': None}
    assert expected_error_dict == test_transfer_log.error_dict
    # manually initialize
    test_transfer_log.load_metadata_table()
    mock_view_df = pd.read_csv(
        mock_view_path, dtype={'subject.label': 'object'}
    )
    mock_view_dict_list = transfer_log.format_flywheel_table(mock_view_df.to_dict(orient='records'))
    test_transfer_log.create_flywheel_table(mock_view_dict_list)
    project = flywheel.Project(group='test_group', label='test_project')
    test_transfer_log.get_path_dict(project)
    # Test match_containers_once option
    test_transfer_log.match_df_records()
    error_df = test_transfer_log.get_error_df()
    assert len(error_df) == 4
    expected_error_messages = [
        'acquisition in flywheel not present in transfer_log',
        '1 more records in flywheel than in transfer_log',
        'acquisition in transfer_log not present in flywheel'
    ]
    for item in expected_error_messages:
        assert item in error_df.values

    # Test match_containers_once of False
    test_transfer_log.match_containers_once = False
    test_transfer_log.match_df_records()
    error_df = test_transfer_log.get_error_df()
    assert len(error_df) == 5


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
