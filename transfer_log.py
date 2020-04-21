"""A Flywheel sdk script to migrate metadata from an xnat csv to a
Flywheel project that was migrated from xnat
"""
import argparse
import csv
import datetime
import itertools
import json
import logging
import os
import re
from abc import ABCMeta, abstractmethod

import flywheel
import pandas as pd
import xlrd
import yaml
from dateutil import tz

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
            raise TransferLogException(
                'Malformed Transfer Log', errors=exc_errors)

    return raw_metadata


def load_flywheel_records(client, config, project_id):
    """Load a dictionary with indexes to easily query the project

    Args:
        client (Client): The flywheel sdk client
        config (Config): The config object
        project_id (str): The id of the project to which to migrate metadata
    Returns:
        list: a list of dictionaries representing flywheel records
    """
    container_type = config.join
    valid_key = '{}.info.transfer_log.valid'.format(container_type)
    deleted_key = '{}.deleted'.format(container_type)
    columns = [query.field for query in config.queries] + \
        [valid_key, deleted_key]
    get_original_timezone = False
    if 'session.timestamp' in columns:
        get_original_timezone = True
        columns.append('session.timezone')
    if container_type == 'acquisition':
        view = client.View(columns=columns, container=container_type,
                           filename=config.filename, process_files=False, match='all',
                           sort=False)
    else:
        view = client.View(columns=columns, sort=False)

    df_dtypes = get_clean_dtypes(client, view, project_id, ignore_cols=[
                                 valid_key, deleted_key])
    flywheel_table = client.read_view_dataframe(
        view, project_id, opts={'dtype': df_dtypes})
    flywheel_table = flywheel_table.astype(df_dtypes)
    # with client.read_view_data(view, project_id) as resp:
    #     flywheel_table = json.load(resp)
    if get_original_timezone:
        flywheel_table['session.timestamp'] = flywheel_table.apply(convert_timezones,
                                                                   axis=1)
    flywheel_table = flywheel_table.to_dict(orient='records')
    return flywheel_table


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
        case_insensitive (bool): if True, string values will be dropped to lower-case for comparison
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
        """Index by which to reference the row (container.id for flywheel, index+2 for transfer log)"""
        pass

    @property
    def match_dict(self):
        """
        Dictionary by which to match table rows

        """
        match_dict = dict()
        for query in self.config.queries:
            if query.value:
                value = self.row_dict.get(
                    query.field) or self.row_dict.get(query.value)
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

    def matches(self, other_table_row):
        """
        Checks if the match_dict dictionaries are equal for both records.

        Args:
            other_table_row: a TableRow to compare

        Returns:
            bool: whether other_table_row matches the record

        """
        return_bool = False

        if self.match_dict == other_table_row.match_dict:
            return_bool = True

        return return_bool

    @abstractmethod
    def get_error_message(self, container_obj):
        """
        Format the error message for the row
        Args:
            container_obj: the flywheel container object if FlywheelRow, None for MetadataRow

        Returns:

        """
        pass

    def get_error_dict(self, error_dict_template, client, dry_run):
        """
        If no matches exist for the record, formats an error dictionary to be written to a csv. Else, returns None
        Args:
            error_dict_template (dict): A dictionary with keys and default values for the return_dict
            client (flywheel.Client): an instance of the flywheel client
            dry_run (bool): if True, do not update flywheel metadata

        Returns:
            (dict or None): a dictionary representing a row to be written to an error log csv.

        """

        if self.match_index:
            return_dict = None
            if not dry_run and isinstance(self, FlywheelRow):
                container_obj = client.get(self.index)
                container_obj.update_info({'transfer_log': {'valid': True}})

        else:
            return_dict = error_dict_template.copy()
            return_dict['row_or_id'] = self.spreadsheet_index
            return_dict.update(self.match_dict)
            return_dict['type'] = self.container_type

            if isinstance(self, FlywheelRow):
                container_obj = client.get(self.index)
                return_dict['error'] = self.get_error_message(container_obj)
                return_dict['path'] = utils.get_resolver_path(
                    client, container_obj)
                valid = container_obj.get('info', {}).get(
                    'transfer_log', {}).get('valid')
                return_dict['label'] = container_obj.get('label')
                if valid:
                    return_dict['error'] = None
            else:
                return_dict['error'] = self.get_error_message(None)

            if not return_dict.get('error'):
                return_dict = None
        return return_dict


class MetadataRow(TableRow):
    """
    Class for representing a row in a transfer log spreadsheet
    """
    @property
    def spreadsheet_index(self):
        """Index by which to reference the row. In this case, index+2 so the row number corresponds to the
            human-readable index in a spreadsheet viewer (accounts for header and index start at 0)
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
            value = datetime.datetime.strptime(
                str(value), query.timeformat).strftime(query.timeformat)

        if query.field == 'subject.label' and isinstance(value, float):
            value = str(int(value))

        value = self.config.mappings.get(str(value), str(value))

        if self.case_insensitive:
            value = value.lower()

        return value

    def get_error_message(self, container_obj=None):
        if self.match_index:
            return None
        message = f'row {self.spreadsheet_index} missing from flywheel'

        return message


class FlywheelRow(TableRow):
    """
    Class for representing a row/record retrieved from a Flywheel DataView
    """

    @property
    def spreadsheet_index(self):
        """Index by which to reference the row. In this case, the id of the flywheel container (the index)"""
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

    def get_error_message(self, container_obj):
        if self.match_index:
            return None
        if not container_obj.get('files'):
            message = f'{self.container_type} in flywheel contains no files'
        else:
            message = f'{self.container_type} in flywheel not present in transfer log'
        return message

#  Encapsulating associated functionality in classes is always a good idea.
#  It would be helpful to know more about its behavior in the docstring.
#  i.e. it looks like this is the central class for identifying, managing and reporting errors.


class TransferLog:
    """
    Class representing a transfer log spreadsheet

    Args:
        client (flywheel.Client): an instance of the flywheel client
        config (transfer_log.Config): object representing transfer_log configuration
        transfer_log_path (str): path to the transfer log spreadsheet
        project_id (str): id of the project container to compare against the transfer log
        case_insensitive (bool): if True, string values will be dropped to lower-case for comparison

    Attributes:
        client (flywheel.Client): an instance of the flywheel client
        config (transfer_log.Config): object representing transfer_log configuration
        transfer_log_path (str): path to the transfer log spreadsheet
        project_id (str): id of the project container to compare against the transfer log
        case_insensitive (bool): if True, string values will be dropped to lower-case for comparison
        flywheel_table (list): list of MetadataRow objects representing the rows in the transfer log
        metadata_table (list): list of Flywheel records retrieved from the project per the config-specified query
        error_list (list): list of error dicts representing to be exported to a csv

    """

    def __init__(self, client, config, transfer_log_path, project_id, case_insensitive):
        self.client = client
        self.config = config
        self.transfer_log_path = transfer_log_path
        self.project_id = project_id

        self.case_insensitive = case_insensitive
        self.flywheel_table = list()
        self.metadata_table = list()
        self.error_list = list()

    @property
    def error_dict(self):
        """The default error dict/template for row errors"""
        return get_template_error_dict(self.config)

    def initialize(self):
        # This "initialize" function could easily be integrated into the TranferLog constructor.
        # And do that much more to reduce overall complexity
        """Parse the transfer log and retrieve the metadata from the Flywheel Project"""
        self.load_metadata_table()
        self.load_flywheel_table()

    def load_metadata_table(self):
        """Parse the transfer log, appending rows as MetadataRows to metadata_table"""
        if not os.path.exists(self.transfer_log_path):
            exc_str = f'{self.transfer_log_path} does not exist. Cannot load transfer log.'
            raise TransferLogException(exc_str)
        else:
            tl_dict_list = load_transfer_log(
                self.transfer_log_path, self.config)
            for index, row_dict in enumerate(tl_dict_list):
                self.metadata_table.append(MetadataRow(
                    self.config, row_dict, index, self.case_insensitive))
        return self.metadata_table

    def load_flywheel_table(self):
        """Load records from Flywheel, appending records as FlywheelRows to flywheel_table"""
        fw_dict_list = load_flywheel_records(
            self.client, self.config, self.project_id)
        for row_dict in fw_dict_list:
            index = row_dict['{}.id'.format(self.config.join)]
            fw_row = FlywheelRow(self.config, row_dict,
                                 index, self.case_insensitive)

            self.flywheel_table.append(fw_row)
        return self.flywheel_table

    def match_fw_to_tl(self):
        """Match FlywheelRow records to MetadataRows"""
        for fw_row in self.flywheel_table:
            result = self.get_metadata(fw_row)
            if result:
                fw_row.match_index = result.spreadsheet_index
                result.match_index = fw_row.spreadsheet_index

    def get_metadata(self, fw_row):
        """Find the MetadataRow matching the FlywheelRow (if any, else return None)"""
        metadata_row = next((item for item in self.metadata_table if (not item.match_index and item.matches(fw_row))),
                            None)
        return metadata_row

    def get_errors(self, dry_run):
        """Compile list of errors for missing, empty and unexpected rows in the transfer log"""
        self.error_list = list()
        for row in self.metadata_table:
            error = row.get_error_dict(self.error_dict, self.client, dry_run)
            if error:
                self.error_list.append(error)
        for row in self.flywheel_table:
            error = row.get_error_dict(self.error_dict, self.client, dry_run)
            if error:
                self.error_list.append(error)
        return self.error_list


def get_template_error_dict(config):
    """
    Formats and returns a template error dict given a transfer_log.Config object
    Args:
        config(transfer_log.Config):

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
    """Loads the yaml file

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
        ignore_cols (list, optional): List of column names to ignore when droping rows with null values

    Returns:
        dict: Dictionary of of data types {column_name: dtype}

    """
    if ignore_cols is None:
        ignore_cols = []
    df_dtypes = {}

    resp = client.read_view_data(
        view, project_id, decode=False, format='json-flat')
    if resp:
        try:
            # data = resp.data
            data_l = eval(resp.data.decode()
                          .replace('null', 'None')
                          .replace('true', 'True')
                          .replace('false', 'False'))
            df = pd.DataFrame(data_l)
            df.drop(ignore_cols, axis=1, inplace=True)
            indexes = list(df[df.isna().any(axis=1)].index)
            for index in sorted(indexes, reverse=True):
                del data_l[index]
            df_dtypes.update(pd.DataFrame(data_l).dtypes.to_dict())
            resp.close()
        except Exception as exc:
            log.warning(
                'An exception raises when trying to clean dtypes\n %s', exc)
            resp.close()

    # replace type with pandas NaN compatible ones
    # see: https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
    for k, v in df_dtypes.items():
        if v == 'int64':
            df_dtypes[k] = 'Int64'

    return df_dtypes


def convert_timezones(row):
    """Modifies the session.timestamp to isoformat UTC from the original timezone,
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
                        datetime.datetime.strptime(
                            str(value), query.timeformat)
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
    else:
        # Extract values from gear_context
        client = gear_context.client
        config_path = gear_context.get_input_path('template')
        metadata = gear_context.get_input_path('transfer_log')
        case_insensitive = gear_context.config.get('case_insensitive')

    # Load in the config yaml input
    config = load_config_file(config_path)

    # set logging level
    log.setLevel(log_level)

    log.debug('Project path is {}'.format(project_path))
    project = client.lookup(project_path)
    transfer_log = TransferLog(
        client, config, metadata, project.id, case_insensitive)
    # This "initialize" function could easily be integrated into the TranferLog constructor.
    transfer_log.initialize()
    transfer_log.match_fw_to_tl()

    # Generate Report
    tl_errors = transfer_log.get_errors(dry_run)

    return tl_errors, list(transfer_log.error_dict.keys())


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
        gear_context_dict = {'client': fw, 'case_insensitive': args.case_insensitive, 'template': args.config,
                             'transfer_log': args.metadata}
        tl_error_list, header_list = main(
            gear_context_dict, script_log_level, path, dry_run=args.dry_run)
        if args.output:
            create_output_file(tl_error_list, args.output, headers=header_list)
        else:
            print(tl_error_list)
    except TransferLogException as e:
        create_output_file(e.errors, 'error-transfer-log.csv',
                           validate_transfer_log=True)
        raise e
