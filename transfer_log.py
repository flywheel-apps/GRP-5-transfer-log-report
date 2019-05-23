"""A Flywheel sdk script to migrate metadata from an xnat csv to a
Flywheel project that was migrated from xnat
"""

import argparse
import csv
import datetime
import flywheel
import logging
import json
import os
import re
import sys
import xlrd
import yaml

import utils

log = logging.getLogger()


CSV_HEADERS = [
    'path',
    'error',
    'type',
    'resolved',
    'label',
    '_id'
]

FLYWHEEL_CONTAINER_TYPES = [
    'group',
    'project',
    'subject',
    'session',
    'acquisition'
]


class Query(object):
    reserved_keys = ['pattern', 'timeformat']
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


class Config(object):
    def __init__(self, config_doc):
        """Object to represent the configurations

        Args:
            config_doc (dict): A dictionary representation of the config
        """
        self.queries = [ Query(q) for q in config_doc.get('query', [])]
        self.join = config_doc.get('join', 'project')
        self.mappings = {}
        for value, keys in config_doc.get('mappings', {}).items():
            for key in keys:
                self.mappings[key] = value


def load_config_file(file_path):
    """Loads the yaml file

    Args:
        file_path (str): path to config file
    Returns:
        Config: A config object
    """
    with open(file_path, 'r') as fp:
        config_doc = yaml.load(fp)

    return Config(config_doc)


def key_from_flywheel(row, config):
    """Convert a flywheel row to a tuple base on what we will be querying

    Args:
        row (dict): A dict returned from the dataview
        config (Config): The config object
    Returns:
        tuple: The key to lookup on
    """
    def format_value(query):
        """Parse a value to a datetime if required by the query

        Args:
            query (Query): A query from the config
        Returns:
            str|NoneType: The formated value
        """
        value = row.get(query.field)
        if query.timeformat is not None:
            try:
                return datetime.datetime.fromisoformat(value).strftime(query.timeformat)
            except ValueError as e:
                raise ValueError('Cannot parse time from non-iso timestamp {}={}'.format(
                    query.field, value
                ))
        elif value is None:
            return value
        else:
            return config.mappings.get(str(value), str(value))

    return tuple([format_value(query) for query in config.queries])


def format_flywheel_key(key, config):
    """Converts a flywheel key to a metadata key

    Args:
        key (tuple): A tuple representing the flywheel key
        config (Config): The config object
    Returns:
        tuple: The key to lookup on
    """
    new_key = []
    for index, key_item in enumerate(key):
        if config.queries[index].value is False:
            new_key.append(None)
        else:
            new_key.append(key_item)
    return tuple(new_key)


def key_from_metadata(row, config):
    """Convert a metadata row to a tuple base on what we will be querying

    Args:
        row (dict): A dict representing a row from the metadata file
        config (Config): The config object
    Returns:
        tuple: The key to lookup on
    """
    def format_value(query):
        """Match a pattern on a value

        Args:
            query (Query): A query from the config
        Returns:
            str|NoneType: The formated value
        """
        if query.value is False:
            # This is a flywheel only field in order to differentiate the acquisitions
            # that don't match a session + modality combo
            return None
        value = row.get(query.value)
        if value is None:
            return value
        elif query.pattern:
            match = re.search(query.pattern, value)
            if match:
                return match.group(0).strip()
        elif query.field == 'subject.label' and isinstance(value, float):
            return str(int(value))
        return str(value)

    return tuple([format_value(query) for query in config.queries])


def get_hierarchy(client, config, project_id):
    """Load a dictionary with indexes to easily query the project

    Args:
        client (Client): The flywheel sdk client
        config (dict): The config dictionary
        project_id (str): The id of th project to migrate metadata to
    Returns:
        dict: a mapping of tuples to flywheel sdk containers
    """
    container_type = config.join
    valid_key = '{}.info.transfer_log.valid'.format(container_type)
    deleted_key = '{}.deleted'.format(container_type)
    columns = [query.field for query in config.queries] + [valid_key, deleted_key]
    if container_type == 'acquisition':
        view = client.View(columns=columns, container=container_type,
                       filename='*.zip', process_files=False, match='all')
    else:
        view = client.View(columns=columns)

    with client.read_view_data(view, project_id) as resp:
        flywheel_table = json.load(resp)

    return {
        key_from_flywheel(row, config): client.get(row['{}.id'.format(container_type)])
        for row in flywheel_table['data']
        if row.get(deleted_key) is None
    }


def load_metadata(metadata_path, config):
    """Loads and formats the metadata to conform to how the flywheel metadata
    object is

    Args:
        metadata_path (str): Path to the metadata file
        config (Config): The config object
    Returns:
        dict: a mapping of tuples to metadata rows
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
    if raw_metadata and not check_config_and_log_match(config, raw_metadata[0]):
        raise Exception('Template file referencing columns not provided in log')

    return {key_from_metadata(row, config): i for i, row in enumerate(raw_metadata)}


def validate_flywheel_against_metadata(flywheel_table, metadata, config):
    """Ensures that a container that matches each row of the metadata exists
    in the project and warns of any container that are not reflected in the
    metadata

    Args:
        flywheel_table (dict): dictionary with flywheel keys
        metadata (dict): dictionary with metadata keys
        config (Config): The transfer log configuration
    Returns:
        tuple: A list of missing container keys, a list of found containers, a
            dictionary of unexpected container keys to the corresponding container
    """
    unexpected_containers = {}
    found_containers_map = {}

    # for container_key in metadata.keys():
    #     if not flywheel_table.get(container_key):
    #         missing_containers.append(container_key)
    #     else:
    #         found_containers.append(flywheel_table.pop(container_key))

    for flywheel_key in flywheel_table.keys():
        formatted_key = format_flywheel_key(flywheel_key, config)
        if not metadata.get(formatted_key):
            if not found_containers_map.get(formatted_key):
                # doesn't match a found row or a yet to be found row
                unexpected_containers[flywheel_key] = flywheel_table[flywheel_key]
            else:
                # Already found, we can just skip over this
                continue
        else:
            # Matches a row that is missing so we move the container over and
            # pop off of the missing rows table
            found_containers_map[formatted_key] = flywheel_table[flywheel_key]
            metadata.pop(formatted_key)

    found_containers = list(found_containers_map.values())

    return metadata, found_containers, unexpected_containers


def create_missing_error(row_number, container_type):
    return {
        'error': 'row {} missing from flywheel'.format(row_number),
        'path': None,
        'type': container_type,
        'resolved': False,
        'label': None,
        '_id': None
    }


def create_unexpected_error(container, client):
    return {
        'error': '{} in flywheel not present in transfer log'.format(container.container_type),
        'path': utils.get_resolver_path(client, container),
        'type': container.container_type,
        'resolved': False,
        'label': container.label,
        '_id': container.id
    }


def check_config_and_log_match(config, row):
    """Ensures that all the columns expected by the config are present in the
        transfer_log unless query value is False

    Args:
        config (Config): The loaded in template file
        row (dict): A single row of the input metadata before being turned into
            keys
    """
    for query in config.queries:
        if query.value not in row.keys() and query.value is not False:
            return False
    return True


def main(client, config_path, log_level, metadata, project_path):
    """Query flywheel for a set of containers base on a tabular file and a
    yaml template on how to use the csv file

    Args:
        client (Client): A flywheel sdk client
        config_path (str): Path to the yaml config file
        log_level (str|int): A logging level (DEBUG, INFO) or int (10, 50)
        metadata (str): Path to the metadata file
        project_path (str): The resolver path to the project
    """

    # Load in the config yaml input
    config = load_config_file(config_path)

    # set logging level
    log.setLevel(log_level)

    # Load in the tabular data
    input_metadata = load_metadata(metadata, config)


    log.debug('Project path is {}'.format(project_path))
    project = client.lookup(project_path)

    # Load in the flywheel hierarchy as tabular data
    flywheel_table = get_hierarchy(client, config, project.id)
    log.debug(flywheel_table.keys())

    missing_containers, found_containers, unexpected_containers = \
        validate_flywheel_against_metadata(flywheel_table, input_metadata, config)

    # Generate Report
    errors = []
    for row_number in missing_containers.values():
        errors.append(create_missing_error(row_number, config.join))
    for container in unexpected_containers.values():
        if not container.info.get('tranfer_log', {}).get('valid'):
            errors.append(create_unexpected_error(container, client))

    for container in found_containers:
        container.update_info({'transfer_log': {'valid': True}})

    return errors


def create_output_file(errors, filename):
    """Outputs the errors into a csv file

    Args:
        errors (list): A list of transfer log errors
        filename (str): Name of the file to output to
    """
    with open(filename, 'w') as output_file:
        csv_dict_writer = csv.DictWriter(output_file, fieldnames=CSV_HEADERS)
        csv_dict_writer.writeheader()
        for error in errors:
            csv_dict_writer.writerow(error)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('path', help='Resolver path of the project')
    parser.add_argument('metadata', help='tabular file containing metadata')
    parser.add_argument('config', help='YAML file to map the xnat metadata')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--api-key', help='Use if not logged in via cli')
    parser.add_argument('--output', '-o', help='Output file csv')

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
        log_level = 'DEBUG'
    else:
        log_level = 'INFO'

    errors = main(fw, args.config, log_level, args.metadata, path)

    if args.output:
        create_output_file(errors, args.output)
    else:
        print(errors)

