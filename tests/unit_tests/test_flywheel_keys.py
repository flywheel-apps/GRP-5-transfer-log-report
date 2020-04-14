import flywheel
import pytest
from unittest.mock import MagicMock
import datetime
import urllib3
import utils
import json

import transfer_log


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
        'w04': 'Week 4',
        'wk4': 'Week 4',
        'Week_4': 'Week 4'
    }


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
