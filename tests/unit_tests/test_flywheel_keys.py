import flywheel
import pytest
from unittest.mock import MagicMock, patch
import datetime
import urllib3
import utils
import json
import numpy as np
import pandas as pd

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


def test_format_json_list_for_python():
    json_list = [
        {"acquisition.id": "5cf7ec6bd9a631002dfddefd", "file.info.SeriesNumber": 'null'},
        {"acquisition.id": "5cf7ec6bd9a631002dfddefd", "file.info.SeriesNumber": 'true'},
        {"acquisition.id": "5cf7ec6bd9a631002dfddefd", "file.info.SeriesNumber": 'false'}
    ]
    exp_list = [
        {"acquisition.id": "5cf7ec6bd9a631002dfddefd", "file.info.SeriesNumber": None},
        {"acquisition.id": "5cf7ec6bd9a631002dfddefd", "file.info.SeriesNumber": True},
        {"acquisition.id": "5cf7ec6bd9a631002dfddefd", "file.info.SeriesNumber": False}
    ]
    assert transfer_log.format_json_list_for_python(json_list) == exp_list


def test_get_data_list():
    client = MagicMock()
    resp_data = [{"acquisition.id": "5cf7ec6bd9a631002dfddefd", "file.info.SeriesNumber": 10},
                 {"acquisition.id": "5cf7ec6bd9a631002dfddefd", "file.info.SeriesNumber": None}]
    resp = urllib3.response.HTTPResponse(body=bytes(json.dumps(resp_data), 'utf-8'))
    client.read_view_data = MagicMock(return_value=resp)
    data_list = transfer_log.get_data_list(client, None, None)
    assert data_list == resp_data


def test_get_df_dtypes():
    data_list = [{"acquisition.id": "5cf7ec6bd9a631002dfddefd", "file.info.SeriesNumber": 10},
                 {"acquisition.id": "5cf7ec6bd9a631002dfddefd", "file.info.SeriesNumber": None}]
    exp_dtypes = {'acquisition.id': np.dtype('O'), 'file.info.SeriesNumber': 'Int64'}
    return_dtypes = transfer_log.get_df_dtypes(data_list)
    assert exp_dtypes == return_dtypes


def test_format_flywheel_table():
    data_list = [
        {
            "acquisition.id": "5cf7ec6bd9a631002dfddefd",
            "file.info.SeriesNumber": 'null',
            'session.timestamp': '2020-01-30 09:33:07',
            'session.timezone': 'null'}
        ,
        {
            "acquisition.id": "5cf7ec6bd9a631002dfddefd",
            "file.info.SeriesNumber": 10,
            'session.timestamp': '2020-01-30 12:34:02',
            'session.timezone': 'null'
        }
    ]
    exp_data_list = [
        {
            "acquisition.id": "5cf7ec6bd9a631002dfddefd",
            "file.info.SeriesNumber": None,
            'session.timestamp': '2020-01-30T09:33:07',
            'session.timezone': None}
        ,
        {
            "acquisition.id": "5cf7ec6bd9a631002dfddefd",
            "file.info.SeriesNumber": 10,
            'session.timestamp': '2020-01-30T12:34:02',
            'session.timezone': None
        }
    ]
    data_list = transfer_log.format_json_list_for_python(data_list)
    formatted_data = transfer_log.format_flywheel_table(data_list)
    assert exp_data_list == formatted_data
