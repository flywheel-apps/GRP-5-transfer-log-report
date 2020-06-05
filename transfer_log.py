#!/usr/bin/python3
"""A Flywheel sdk script to migrate metadata from an xnat csv to a
Flywheel project that was migrated from xnat
"""
from abc import ABCMeta, abstractmethod
import argparse
import backoff
import csv
import datetime
import json
import logging
import os
import re

import backoff
from dateutil import tz
import flywheel
import numpy as np
import pandas as pd
import xlrd
import yaml

import utils

log = logging.getLogger()

CSV_HEADERS = [
    'row_or_id',
    'path',
    'error',
    'type',
    'resolved',
    'label'
]

ERROR_DICT = {
    'error': None,
    'path': None,
    'type': None,
    'resolved': False,
    'label': None,
    'row_or_id': None
}

TRANSFER_LOG_ERROR_HEADERS = [
    'row',
    'column',
    'error'
]

FLYWHEEL_CONTAINER_TYPES = [
    'group',
    'project',
    'subject',
    'session',
    'acquisition'
]


class TransferLogException(Exception):
    def __init__(self, msg, errors=[]):
        self.errors = errors
        super(TransferLogException, self).__init__(msg)


class Query(object):
    reserved_keys = ['pattern', 'timeformat', 'timezone', 'validate']

    def __init__(self, document):
        """Object to represent a single query

        Args:
            document (dict): The dictionary from the config file
        """
        fields = list(set(document.keys()) - set(Query.reserved_keys))
        if len(fields) != 1:
            raise ValueError('Malformed query!')

        self.field = fields[0]
        self.value = document.get(self.field)
        self.pattern = document.get('pattern')
        self.timeformat = document.get('timeformat')
        self.timezone = document.get('timezone')
        self.validate = document.get('validate', self.pattern)


class Config(object):
    def __init__(self, config_doc):
        """Object to represent the configurations

        Args:
            config_doc (dict): A dictionary representation of the config
        """
        self.queries = []
        self.default_queries = {
            'acquisition.id': Query({'acquisition.id': False})
        }
        for query_doc in config_doc.get('query', []):
            query = Query(query_doc)
            self.queries.append(query)
            # Remove default query if it's being set
            self.default_queries.pop(query.field, None)
        self.field_dict = dict()
        for query in self.queries:
            if query.value:
                self.field_dict[query.value] = query.field
        self.queries += self.default_queries.values()

        self.join = config_doc.get('join', 'session')
        self.filename = config_doc.get('filename', '*.zip')
        self.mappings = {}
        for value, keys in config_doc.get('mappings', {}).items():
            for key in keys:
                if key in self.mappings:
                    self.mappings[value] = self.mappings.get(key)
                else:
                    self.mappings[key] = value


def load_transfer_log(metadata_path, config):
    """Loads and formats the transfer log spreadsheet.

    Args:
        metadata_path (str): Path to the metadata file
        config (Config): The config object
    Returns:
        list: list of dicts representing the transfer log rows
    """
    raw_metadata = []

    extension = os.path.splitext(metadata_path)[1]
    if extension == '.xlsx':
        wb = xlrd.open_workbook(metadata_path)
        sh = wb.sheet_by_index(0)
        keys = None
        for row in sh.get_rows():
            if keys is None:
                keys = [cell.value for cell in row]
            else:
                raw_metadata.append({
                    keys[i]: row[i].value for
                    i in range(len(keys))
                })
    elif extension == '.csv':
        with open(metadata_path, 'r') as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                raw_metadata.append(row)
    else:
        raise Exception('Filetype "%s" not supported', extension)
    if raw_metadata:
        exc_errors = check_config_and_log_match(config, raw_metadata)
        if exc_errors:
            raise TransferLogException('Malformed Transfer Log', errors=exc_errors)

    return raw_metadata


@backoff.on_exception(backoff.expo, flywheel.rest.ApiException,
                      max_time=300, giveup=utils.false_if_status_gte_500)
def get_view_from_config(fw_client, config):
    """
    Constructs and returns a DataView according to config's specification

    Args:
        fw_client (flywheel.Client): an instance of the flywheel client
        config (transfer_log.Config): config option representing a template
            file

    Returns:
        flywheel.DataView: a data view configured according to config
    """
    container_type = config.join
    valid_key = '{}.info.transfer_log.valid'.format(container_type)
    deleted_key = '{}.deleted'.format(container_type)
    columns = [query.field for query in config.queries] + \
              [valid_key, deleted_key]
    # Include file.name if file attributes are in query.field
    if [column for column in columns if column.startswith('file.')]:
        columns.append('file.name')
    if 'session.timestamp' in columns:
        columns.append('session.timezone')

    if container_type == 'acquisition':
        view = fw_client.View(
            columns=columns, container=container_type,
            filename=config.filename, process_files=False,
            match='all', sort=False
        )
    else:
        view = fw_client.View(columns=columns, sort=False)

    return view


class TableRow(object):
    """
    Abstract class for representing a record

    Args:
        config (transfer_log.Config): object representing transfer_log configuration
        row_dict (dict): a dictionary representing the record
        index: index at which the record was found (container for FlywheelRow, int for MetadataRow
        case_insensitive (bool): if True, string values will be dropped to lower-case for comparison

    Attributes:
        config (transfer_log.Config): object representing transfer_log configuration
        row_dict (dict): a dictionary representing the record
        index: index at which the record was found (container for FlywheelRow, int for MetadataRow
        case_insensitive (bool): if True, string values will be dropped to lower-case for
            comparison
        match_index: index for the matching record from flywheel/transfer log

    """
    __metaclass__ = ABCMeta

    def __init__(self, config, row_dict, index, case_insensitive):
        self.config = config
        self.row_dict = row_dict
        self.index = index
        self.case_insensitive = case_insensitive
        self.match_index = None

    @property
    @abstractmethod
    def spreadsheet_index(self):
        """
        Index by which to reference the row (container.id for flywheel, index+2 for transfer log)
        """
        pass

    @property
    def match_dict(self):
        """
        Dictionary by which to match table rows

        """
        match_dict = dict()
        for query in self.config.queries:
            if query.value:
                value = self.row_dict.get(query.field) or self.row_dict.get(query.value)
                value = self.format_value(query, value)
                match_dict[query.field] = value
        return match_dict

    @property
    def container_type(self):
        """The Flywheel container type of the row."""
        container_type = self.config.join
        return container_type

    @abstractmethod
    def format_value(self, query, value):
        """Format the value based on the query object"""
        pass


class MetadataRow(TableRow):
    """
    Class for representing a row in a transfer log spreadsheet
    """

    @property
    def spreadsheet_index(self):
        """
        Index by which to reference the row. In this case, index+2 so the row number corresponds
            to the human-readable index in a spreadsheet viewer (accounts for header and
            index start at 0)
        """
        return self.index + 2

    def format_value(self, query, value):
        if value is None:
            return value

        if query.pattern:
            match = re.search(query.pattern, value)
            if match:
                try:
                    value = match.group(0).strip()
                except AttributeError:
                    pass

        if query.timeformat:
            value = datetime.datetime.strptime(str(value), query.timeformat).strftime(
                query.timeformat
            )

        if query.field == 'subject.label' and isinstance(value, float):
            value = str(int(value))

        value = self.config.mappings.get(str(value), str(value))

        if self.case_insensitive:
            value = value.lower()

        return value


class FlywheelRow(TableRow):
    """
    Class for representing a row/record retrieved from a Flywheel DataView
    """

    @property
    def spreadsheet_index(self):
        """
        Index by which to reference the row. In this case, the id of the flywheel container
            (the index)
        """
        container_id = self.index

        return container_id

    def format_value(self, query, value):
        if value is None:
            return value

        if query.timeformat is not None:
            try:
                timestamp = datetime.datetime.fromisoformat(value)
                timezone = tz.gettz(query.timezone)
                if timezone and query.timezone:
                    timestamp = timestamp.astimezone(timezone)
                value = timestamp.strftime(query.timeformat)
            except ValueError as exc:
                raise ValueError('Cannot parse time from non-iso timestamp {}={} due to {}'.format(
                    query.field, value, exc
                ))
        else:
            value = self.config.mappings.get(str(value), str(value))

        if self.case_insensitive:
            value = value.lower()

        return value


class TransferLog:
    """
    Class representing a transfer log spreadsheet

    Args:
        client (flywheel.Client): an instance of the flywheel client
        config (transfer_log.Config): object representing transfer_log configuration
        transfer_log_path (str): path to the transfer log spreadsheet
        project_id (str): id of the project container to compare against the transfer log
        case_insensitive (bool): if True, string values will be dropped to lower-case for
            comparison
        match_containers_once (bool): if True, excludes errors for Flywheel ids
            that match transfer_log rows

    Attributes:
        client (flywheel.Client): an instance of the flywheel client
        config (transfer_log.Config): object representing transfer_log configuration
        transfer_log_path (str): path to the transfer log spreadsheet
        project_id (str): id of the project container to compare against the transfer log
        case_insensitive (bool): if True, string values will be dropped to lower-case for
            comparison
        flywheel_table (list): list of MetadataRow objects representing the rows in the
            transfer log
        metadata_table (list): list of Flywheel records retrieved from the project per the
            config-specified query
        matched_containers (list): list of Flywheel container ids that match
            transfer log rows
        empty_containers (list): list of Flywheel container ids of container
            type self.config.join that are missing files
        match_cols (list): list of fields/columns matched between flywheel and
            the transfer log
        resolver_path_dict (dict): dictionary mapping Flywheel ids to resolver
            paths
        flywheel_df (pandas.DataFrame): dataframe containing flywheel records
            loaded from the dataview
        metadata_df (pandas.DataFrame): dataframe containing metadata records
            loaded from the transfer log
        match_df (pandas.DataFrame): dataframe resulting from merging flywheel
            and metadata record count dataframes

    """

    def __init__(self, client, config, transfer_log_path, project_id,
                 case_insensitive=False, match_containers_once=False):
        self.client = client
        self.config = config
        self.transfer_log_path = transfer_log_path
        self.project_id = project_id

        self.case_insensitive = case_insensitive
        self.match_containers_once = match_containers_once
        self.flywheel_table = list()
        self.metadata_table = list()
        self.matched_containers = list()
        self.empty_containers = list()
        self.match_cols = [
            query.field for query in self.config.queries if query.value
        ]
        self.resolver_path_dict = dict()
        self.flywheel_df = None
        self.metadata_df = None
        self.match_df = None

    @property
    def error_dict(self):
        """The default error dict/template for row errors"""
        return get_template_error_dict(self.config)

    def initialize(self):
        """Parse the transfer log and retrieve the metadata from the Flywheel Project"""
        log.info('Loading transfer log records...')
        self.load_metadata_table()
        log.info('Loading Flywheel records...')
        self.load_flywheel_table()
        log.info('Matching Flywheel and transfer log records...')
        self.match_df_records()
        log.info('Loading project resolver paths from Flywheel...')
        project = self.client.get_project(self.project_id)
        self.resolver_path_dict = self.get_path_dict(project)
        log.info('Identifying empty containers...')
        self.empty_containers = self.get_empty_container_ids(
            self.client,
            self.project_id,
            self.config.join
        )

    def load_metadata_table(self):
        """Parse the transfer log, appending rows as MetadataRows to metadata_table"""
        if not os.path.exists(self.transfer_log_path):
            exc_str = f'{self.transfer_log_path} does not exist. Cannot load transfer log.'
            raise TransferLogException(exc_str)
        else:
            tl_dict_list = load_transfer_log(self.transfer_log_path, self.config)
            for index, row_dict in enumerate(tl_dict_list):
                self.metadata_table.append(
                    MetadataRow(self.config, row_dict, index, self.case_insensitive)
                )
        self.metadata_df = self.get_table_df(self.metadata_table)
        return self.metadata_table

    def load_flywheel_table(self):
        """Load records from Flywheel, appending records as FlywheelRows to flywheel_table"""
        fw_dict_list = get_flywheel_records(self.client, self.config, self.project_id)
        self.create_flywheel_table(fw_dict_list)
        return self.flywheel_table

    def create_flywheel_table(self, fw_dict_list):
        """Load the dict_list as FlywheelRows"""
        for row_dict in fw_dict_list:
            index = row_dict['{}.id'.format(self.config.join)]
            fw_row = FlywheelRow(self.config, row_dict, index, self.case_insensitive)

            self.flywheel_table.append(fw_row)
        self.flywheel_df = self.get_table_df(self.flywheel_table)
        return self.flywheel_table

    @staticmethod
    def get_table_df(table):
        """
        Assemble a DataFrame from TableRow match_dict values
        Args:
            table (list): list TableRow objects

        Returns:
            pandas.DataFrame
        """
        # Assemble list of dicts for conversion to df
        dict_list = list()
        for row in table:
            copy_dict = row.match_dict.copy()
            # Add index so we can refer to it when generating errors
            copy_dict['tl_index'] = row.index
            if row.row_dict.get('file.name') and \
                    'file.name' not in copy_dict.keys():
                copy_dict['file.name'] = row.row_dict.get('file.name')
            dict_list.append(copy_dict)
        df = pd.DataFrame(dict_list)

        return df

    @staticmethod
    def get_rel_path(row_dict):
        """
        Gets the relative resolver path from a row_dict (dataview result)
        Args:
            row_dict (dict): dictionary representing a dataview row returned by
                Flywheel

        Returns:
            str: the relative path for the container (does not include group or
                project)
        """
        label_list = [
            row_dict.get('subject.label'),
            row_dict.get('session.label'),
            row_dict.get('acquisition.label')
        ]
        label_list = list(filter(None, label_list))
        rel_path = '/'.join(label_list)
        return rel_path

    def get_path_dict(self, project):
        """
        Creates a dictionary with container id: resolver path key: value pairs

        Args:
            project (flywheel.Project): the Flywheel project

        Returns:
            dict: a dictionary with container id: resolver path key: value
                pairs
        """
        project_path = '/'.join([project.group, project.label])
        path_dict = dict()
        for row in self.flywheel_table:
            rel_path = self.get_rel_path(row.row_dict)
            res_path = '/'.join([project_path, rel_path])
            id_str = f'{self.config.join}.id'
            container_id = row.row_dict.get(id_str)
            path_dict[container_id] = res_path
        self.resolver_path_dict = path_dict
        return path_dict

    def get_record_df(self, df):

        # Squash dataframe into one row per set of match column values
        df = df.groupby(self.match_cols)['tl_index'].apply(list).reset_index()
        # ids cannot be duplicated in the list if we're only matching once
        if self.match_containers_once:
            df['tl_index'] = df['tl_index'].apply(lambda x: list(set(x)))
        # Add count of records
        df['records'] = df['tl_index'].apply(len)
        return df

    @staticmethod
    def get_empty_container_ids(fw_client, project_id, container_type):
        """
        Retrieves a list of empty containers of container_type in the Flywheel
            project with id project_id
        Args:
            fw_client (flywheel.Client): an instance of the flywheel client
            project_id (str): id belonging to a Flywheel project
            container_type (str): 'session', 'subject', or 'acquisition'

        Returns:
            list: list of container ids that do not have files
        """
        container_list = list()
        query = f'parents.project={project_id},files.size=null'
        if container_type == 'acquisition':
            container_list = [
                res for res in fw_client.acquisitions.iter_find(query)
            ]
        elif container_type == 'session':
            container_list = [
                res for res in fw_client.sessions.iter_find(query)
            ]
        elif container_type == 'subject':
            container_list = [
                res for res in fw_client.subjects.iter_find(query)
            ]
        else:
            log.error(
                'Unexpected container type %s - cannot find empty containers',
                container_type
            )
        if container_list:
            id_list = [res.id for res in container_list]
        else:
            id_list = list()
        return id_list

    def get_match_row_error(self, row):
        """
        Creates an error message for a row in self.match_df
        Args:
            row (pandas.Series): a row from self.match_df
        Returns:
            str: the error message
        """
        error_msg = None
        container_type = self.config.join
        empty_id_list = self.empty_containers
        missing_str = '{} in {} not present in {}'
        unequal_str = '{} more records in {} than in {}'
        # Records in flywheel, not in transfer log
        if row['_merge'] == 'left_only':
            error_msg = missing_str.format(
                container_type, 'flywheel', 'transfer_log'
            )
            # Address empty containers specifically
            if empty_id_list and isinstance(row['tl_index_flywheel'], list):
                if list(set(empty_id_list) & set(row['tl_index_flywheel'])):
                    error_msg = f'{container_type} in flywheel contains no files'

        # Records in transfer log, but not in flywheel
        elif row['_merge'] == 'right_only':
            error_msg = missing_str.format(
                container_type, 'transfer_log', 'flywheel'
            )
        # Records that match at least once between flywheel and transfer_log
        elif row['_merge'] == 'both':
            diff = int(row['records_flywheel'] - row['records_metadata'])
            # More records in flywheel
            if diff > 0:

                error_msg = unequal_str.format(
                    str(diff), 'flywheel', 'transfer_log'
                )

            # More records in transfer_log
            elif diff < 0:
                error_msg = unequal_str.format(
                    str(abs(diff)), 'transfer_log', 'flywheel'
                )
            # No error - flywheel and transfer_log have equal counts
            else:
                error_msg = None
        # This won't happen via pd.merge, but let's complete the logic
        else:
            error_msg = 'merge value not recognized: {}'.format(row['_merge'])
        return error_msg

    def get_error_df(self):
        """
        Creates a dataframe describing errors/inconsistencies between the
            Transfer Log and the Flywheel project
        Returns:
            pandas.DataFrame
        """
        # Copy so we don't transform match_df
        error_df = self.match_df.copy()
        # Get the error messages
        error_df['error'] = error_df.apply(
            self.get_match_row_error,
            axis='columns'
        )
        # Drop rows without errors
        error_df = error_df[error_df['error'].notnull()]

        # Copy tl_index_flywheel in preparation for transform to row per id
        error_df['matching_fw_ids'] = error_df['tl_index_flywheel'].copy()

        # Transform to row for each fw id
        error_df = error_df.explode('tl_index_flywheel').reset_index(drop=True)

        # If match_containers_once, drop fw rows that already have tl matches
        if self.match_containers_once:
            error_df = error_df[
                ~((error_df['tl_index_flywheel'].isin(
                    self.matched_containers
                )) & (error_df['_merge'] != 'both'))
            ].reset_index(drop=True)

        # Get the resolver paths for flywheel IDs
        error_df['path'] = error_df['tl_index_flywheel'].map(
            self.resolver_path_dict
        )

        # Rename some columns
        error_df = error_df.rename(
            {
                'tl_index_flywheel': 'flywheel_id',
                'tl_index_metadata': 'transfer_log_rows'
            },
            axis='columns'
        )
        # Set columns and column order
        column_list = self.match_cols.copy()
        column_list = ['flywheel_id', 'transfer_log_rows'] + column_list
        column_list = column_list + ['error', 'matching_fw_ids', 'path']
        error_df = error_df[column_list]

        return error_df

    @staticmethod
    def count_df_errors(df):
        """
        Counts the number of unique flywheel container IDs and Transfer Log
            row indices, given an error_df generated by get_error_df

        Args:
            df (pandas.DataFrame): a dataframe generated by
                the get_error_df method

        Returns:
            int: the count of unique flywheel container IDs and Transfer Log
                row indices
        """
        tl_index_list = list()
        for val in df['transfer_log_rows'].values:
            if isinstance(val, list):
                tl_index_list.extend(val)

        tl_index_list = list(set(tl_index_list))
        fw_index_list = [
            fw_id for fw_id in df['flywheel_id'].unique()
            if isinstance(fw_id, str)
        ]
        err_count = len(fw_index_list) + len(tl_index_list)
        return err_count

    def match_df_records(self):
        # Collapse on match field values, add count for records
        fw_record_df = self.get_record_df(self.flywheel_df)
        meta_record_df = self.get_record_df(self.metadata_df)
        # Merge on match field values
        self.match_df = pd.merge(
            fw_record_df, meta_record_df,
            how='outer', on=self.match_cols,
            indicator=True, suffixes=('_flywheel', '_metadata')
        )
        # replace NA with 0 for record counts
        self.match_df['records_metadata'].fillna(0, inplace=True)
        self.match_df['records_flywheel'].fillna(0, inplace=True)

        # select rows where transfer log and flywheel match
        both_df = self.match_df[self.match_df['_merge'] == 'both']
        # Get list of container IDs with transfer log matches
        self.matched_containers = list(set([
            x for array in both_df['tl_index_flywheel'] for x in array
        ]))
        return self.match_df


def get_template_error_dict(config):
    """
    Formats and returns a template error dict given a transfer_log.Config object
    Args:
        config(transfer_log.Config): config object representing template file
            input

    Returns:
        dict: template error dict with default null values
    """
    error_global_copy = ERROR_DICT.copy()
    error_dict = dict()
    error_dict['row_or_id'] = error_global_copy.pop('row_or_id')
    for query in config.queries:
        if query.value:
            error_dict[query.field] = None
    for key, value in error_global_copy.items():
        error_dict[key] = value
    return error_dict


def load_config_file(file_path):
    """
    Loads the yaml file template

    Args:
        file_path (str): path to config file
    Returns:
        Config: A config object
    """
    with open(file_path, 'r') as fp:
        config_doc = yaml.load(fp, Loader=yaml.SafeLoader)

    return Config(config_doc)


def get_clean_dtypes(client, view, project_id, ignore_cols=None):
    """Returns curated dtypes dictionary

    Given view, retrieve data from API, remove null values and infer data type and "null-tolerant"
    pandas data type of remaining rows

    Args:
        client (flywheel.Client): Flywheel client
        view (object): Flywheel dataview
        project_id (str): Flywheel project ID
        ignore_cols (list, optional): List of column names to ignore when droping rows with null
            values

    Returns:
        dict: Dictionary of of data types {column_name: dtype}

    """
    if ignore_cols is None:
        ignore_cols = []
    df_dtypes = {}
    resp = client.read_view_data(view, project_id, decode=False, format='json-flat')
    if resp:
        try:
            # data = resp.data
            data_l = eval(resp.data.decode()
                          .replace('null', 'None')
                          .replace('true', 'True')
                          .replace('false', 'False'))
            resp.close()
            df = pd.DataFrame(data_l)
            df.drop(ignore_cols, axis=1, inplace=True)
            indexes = list(df[df.isna().any(axis=1)].index)
            for index in sorted(indexes, reverse=True):
                del data_l[index]
            df_dtypes.update(pd.DataFrame(data_l).dtypes.to_dict())
            resp.close()
        except Exception as exc:
            log.warning('An exception raises when trying to clean dtypes\n %s', exc)
            resp.close()

    # replace type with pandas NaN compatible ones
    # see: https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
    for k, v in df_dtypes.items():
        if v == 'int64':
            df_dtypes[k] = 'Int64'

    return df_dtypes


def get_df_dtypes(data_list, ignore_cols=None):
    df = pd.DataFrame(data_list)
    # copy so we don't mutate the original list
    data_list_copy = data_list.copy()
    if ignore_cols is None:
        ignore_cols = []
    df_dtypes = {}
    df.drop(ignore_cols, axis=1, inplace=True)
    # drop empty columns
    df.dropna(axis=1, how='all', inplace=True)
    indexes = list(df[df.isna().any(axis=1)].index)
    for index in sorted(indexes, reverse=True):
        del data_list_copy[index]
    df_dtypes.update(pd.DataFrame(data_list_copy).dtypes.to_dict())

    # replace type with pandas NaN compatible ones
    # see: https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
    for k, v in df_dtypes.items():
        if v == 'int64':
            df_dtypes[k] = 'Int64'

    return df_dtypes


@backoff.on_exception(backoff.expo, flywheel.rest.ApiException,
                      max_time=300, giveup=utils.false_if_status_gte_500)
def get_data_list(fw_client, data_view, container_id):
    """
    Returns view rows for a container from flywheel as a list of dicts
    Args:
        fw_client (flywheel.Client): an instance of the flywheel client
        data_view (flywheel.DataView): the data view for which to retrieve data
        container_id (str): flywheel container id

    Returns:
        list: list of dicts representing view rows
    """
    log.debug('Loading data view for %s', container_id)

    data_view_response = fw_client.read_view_data(
        data_view, container_id, decode=False, format='json-flat'
    )

    response_json = json.loads(data_view_response.data.decode())
    data_view_response.close()
    response_json = format_json_list_for_python(response_json)

    return response_json


def format_json_list_for_python(json_list):
    """
    Given an input list of flat dicts, returns dictionary with str values of
        true, false, null replaced with True/False booleans and None, respectively
    Args:
        json_list: list of flat diction

    Returns:
        list: a corrected list of dicts with booleans and None in the place of true/false/null strs
    """
    replace_dict = {'true': True, 'false': False, 'null': None}
    new_list = list()
    for idict in json_list:
        dict_copy = idict.copy()
        for key, value in idict.items():
            if isinstance(value, str):
                value = replace_dict.get(value.lower(), value)
                dict_copy[key] = value
        new_list.append(dict_copy)
    return new_list


def format_flywheel_table(row_dict_list, ignore_cols=None):
    """
    Fix the dtypes and timestamps where applicable for row_dict_list
    Args:
        row_dict_list (list): list of dicts representing flywheel dataview rows
            for a container
        ignore_cols (list): list of columns to exclude when formatting dtypes

    Returns:
        list: row_dict_list with properly handled dtypes and timestamps
    """
    df = pd.DataFrame(row_dict_list)
    # Fix dtypes
    dtypes = get_df_dtypes(row_dict_list, ignore_cols)
    if dtypes:
        df = df.astype(dtypes)
        df.replace({np.nan: None}, inplace=True)
    # Handle timezones
    if 'session.timestamp' in df.columns:
        df['session.timestamp'] = df.apply(convert_timezones, axis=1)
    flywheel_table = df.to_dict(orient='records')
    return flywheel_table


def get_flywheel_records(fw_client, config, project_id):
    """
    Load records for a Flywheel project with id project_id according to config
    Args:
        fw_client (flywheel.Client): an instance of the flywheel client
        config (transfer_log.Config): config object representing template file
            input
        project_id (str): flywheel container id

    Returns:
        list: a formatted list of dicts retrieved from flywheel for a dataview
            constructed according to config
    """
    container_type = config.join
    valid_key = '{}.info.transfer_log.valid'.format(container_type)
    deleted_key = '{}.deleted'.format(container_type)
    ignore_cols = [valid_key, deleted_key]
    view = get_view_from_config(fw_client, config)
    data_list = list()
    project = fw_client.get_project(project_id)
    for subject in project.subjects.iter():
        tmp_list = get_data_list(
            fw_client=fw_client, data_view=view, container_id=subject.id
        )
        data_list.extend(tmp_list)
    flywheel_table = format_flywheel_table(data_list, ignore_cols=ignore_cols)
    return flywheel_table


def convert_timezones(row):
    """
    Modifies the session.timestamp to isoformat UTC from the original timezone,
        given by session.timezone

    Args:
        row (pandas.Series): A single row in a dataframe flywheel table

    Returns
        str: The session timestamp in the correct UTC
    """
    if row['session.timestamp'] is not None:
        if isinstance(row['session.timezone'], str):
            return datetime.datetime.fromisoformat(row['session.timestamp']).astimezone(
                tz.gettz(row['session.timezone'])).isoformat()
        else:
            return datetime.datetime.fromisoformat(row['session.timestamp']).isoformat()
    else:
        return row['session.timestamp']


def check_config_and_log_match(config, raw_metadata):
    """Ensures that all the columns expected by the config are present in the
        transfer_log unless query value is False

    Args:
        config (Config): The loaded in template file
        raw_metadata (list): A list of rows, which are represented as dicts

    Returns:
        list: List of malformed transfer log errors
    """
    error_list = []
    header = raw_metadata[0].keys()
    for query in config.queries:
        if query.value not in header and query.value is not False:
            error_list.append({
                'column': query.value,
                'error': 'Transfer log missing column {}'.format(query.value)
            })

    if not error_list:
        for index, row in enumerate(raw_metadata):
            for query in config.queries:
                value = None
                if query.validate:
                    match = re.search(query.validate, str(row[query.value]))
                    if not match:
                        error_list.append({
                            'row': index + 2,
                            'column': query.value,
                            'error': 'Value {} does not match {}'.format(row[query.value],
                                                                         query.validate)
                        })
                    try:
                        value = match.group(0).strip()
                    except AttributeError:
                        pass
                elif query.value is not False:
                    value = str(row[query.value])
                if query.timeformat and value is not None:
                    try:
                        datetime.datetime.strptime(str(value), query.timeformat)
                    except Exception:
                        error_list.append({
                            'row': index + 2,
                            'column': query.value,
                            'error': 'Timeformat {} does not match {}'.format(value,
                                                                              query.timeformat)
                        })

    return error_list


def main(gear_context, log_level, project_path, dry_run=False):
    """Query flywheel for a set of containers base on a tabular file and a
        yaml template on how to use the csv file

        Args:
            gear_context (GearContext): the flywheel gear context object
            log_level (str|int): A logging level (DEBUG, INFO) or int (10, 50)
            project_path (str): The resolver path to the project
            dry_run (bool): whether to update info.transfer_log.valid on containers that are valid

        """
    if isinstance(gear_context, dict):
        client = gear_context.get('client')
        config_path = gear_context.get('template')
        metadata = gear_context.get('transfer_log')
        case_insensitive = gear_context.get('case_insensitive')
        match_containers_once = gear_context.get('match_containers_once')
    else:
        # Extract values from gear_context
        client = gear_context.client
        config_path = gear_context.get_input_path('template')
        metadata = gear_context.get_input_path('transfer_log')
        case_insensitive = gear_context.config.get('case_insensitive')
        match_containers_once = gear_context.config.get('match_containers_once')

    # Load in the config yaml input
    config = load_config_file(config_path)

    # set logging level
    log.setLevel(log_level)

    log.debug('Project path is {}'.format(project_path))
    project = client.lookup(project_path)
    transfer_log = TransferLog(client, config, metadata, project.id, case_insensitive,
                               match_containers_once)
    transfer_log.initialize()
    error_df = transfer_log.get_error_df()
    error_count = transfer_log.count_df_errors(error_df)
    return error_df, error_count


def create_output_file(errors, filename, validate_transfer_log=False, headers=None):
    """Outputs the errors into a csv file

    Args:
        errors (list): A list of transfer log errors
        filename (str): Name of the file to output to
        validate_transfer_log (bool): Use headers for malformed transfer log
        headers (list): list of headers to use for spreadsheet
    """
    if not headers:
        headers = TRANSFER_LOG_ERROR_HEADERS if validate_transfer_log else CSV_HEADERS
    with open(filename, 'w') as output_file:
        csv_dict_writer = csv.DictWriter(output_file, fieldnames=headers)
        csv_dict_writer.writeheader()
        for error in errors:
            csv_dict_writer.writerow(error)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('path', help='Resolver path of the project')
    parser.add_argument('metadata', help='tabular file containing metadata')
    parser.add_argument('config', help='YAML file to map the xnat metadata')
    parser.add_argument('--case_insensitive', action='store_true')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--api-key', help='Use if not logged in via cli')
    parser.add_argument('--output', '-o', help='Output file csv')
    parser.add_argument('--dry-run', action='store_true',
                        help='Will not update validity of transfer log')
    parser.add_argument('--match-once', action='store_true',
                        help='Do not log errors for multiple container files matching fw row')
    args = parser.parse_args()
    # Path may be fw://<group_id>/<project_label>
    path = args.path.split('//')[-1]

    # Create client
    if args.api_key:
        fw = flywheel.Client(args.api_key)
    else:
        fw = flywheel.Client()

    # Set logging level with verbosity
    if args.verbose:
        script_log_level = 'DEBUG'
    else:
        script_log_level = 'INFO'

    tl_error_list = None
    try:
        gear_context_dict = {'client': fw,
                             'case_insensitive': args.case_insensitive,
                             'template': args.config,
                             'transfer_log': args.metadata,
                             'match_containers_once': args.match_once}
        tl_error_df, tl_error_count = main(gear_context_dict,
                                           script_log_level,
                                           path,
                                           dry_run=args.dry_run)
        if args.output:
            tl_error_df.to_csv(args.output, index=False)
        else:
            print(tl_error_df)
    except TransferLogException as e:
        create_output_file(e.errors, 'error-transfer-log.csv',
                           validate_transfer_log=True)
        raise e
