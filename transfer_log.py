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
            return str(value)

    return tuple([format_value(query) for query in config.queries])


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
        value = row.get(query.value)
        if value is None:
            return value
        elif query.pattern:
            match = re.search(query.pattern, value)
            if match:
                return match.group(0).strip()
        return str(value)

    return tuple([format_value(query) for query in config.queries])


def get_hierarchy(client, config, project_id):
    """Load a dictionary with indexes to easily query the project

    Args:
        client (Client): The flywheel sdk client
        config (dict): The config dictionary
        project_id (str): The id of th project to migrate metadata to
    Returns:
        dict: a mapping of tuples to dataview rows
    """
    container_type = config.join
    columns = [query.field for query in config.queries]
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

    return {key_from_metadata(row, config): row for row in raw_metadata}


def validate_flywheel_against_metadata(flywheel_table, metadata):
    """Ensures that a container that matches each row of the metadata exists
    in the project and warns of any container that are not reflected in the
    metadata

    Args:
        flywheel_table (dict): dictionary with flywheel keys
        metadata (dict): dictionary with metadata keys
    Returns:
        tuple: A list of missing container keys, a list of found containers, a
            dictionary of unexpected container keys to the corresponding container
    """
    missing_containers = []
    found_containers = []

    for container_key in metadata.keys():
        if not flywheel_table.get(container_key):
            missing_containers.append(container_key)
        else:
            found_containers.append(flywheel_table.pop(container_key))

    return missing_containers, found_containers, flywheel_table


def create_missing_error(container_key, container_type):
    return {
        'error': '{} {} missing from flywheel'.format(container_type,
                                                         container_key),
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


def main(client, config_path, log_level, metadata, project_label):
    """Query flywheel for a set of containers base on a tabular file and a
    yaml template on how to use the csv file

    Args:
        client (Client): A flywheel sdk client
        config_path (str): Path to the yaml config file
        log_level (str|int): A logging level (DEBUG, INFO) or int (10, 50)
        metadata (str): Path to the metadata file
        project_label (str): The label of the project to validate
    """

    # Load in the config yaml input
    config = load_config_file(config_path)

    # set logging level
    log.setLevel(log_level)

    # Load in the tabular data
    input_metadata = load_metadata(metadata, config)


    log.debug('Project label is {}'.format(project_label))
    project = client.projects.find_one('label={}'.format(project_label))

    # Load in the flywheel hierarchy as tabular data
    flywheel_table = get_hierarchy(client, config, project.id)

    missing_containers, found_containers, unexpected_containers = \
        validate_flywheel_against_metadata(flywheel_table, input_metadata)

    # Generate Report
    errors = []
    for container in missing_containers:
        errors.append(create_missing_error(container, config.join))
    for container in unexpected_containers.values():
        errors.append(create_unexpected_error(container, client))

    for container in found_containers:
        container.update_info({'transfer_log': {'valid': True}})

    return errors


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('group', help='Id of the group the project is in')
    parser.add_argument('project', help='Label of the project to migrate to')
    parser.add_argument('metadata', help='tabular file containing metadata')
    parser.add_argument('config', help='YAML file to map the xnat metadata')
    parser.add_argument('--verbose', '-v', action='store_true')
    parser.add_argument('--api-key', help='Use if not logged in via cli')

    args = parser.parse_args()

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

    print(main(fw, args.config, log_level, args.metadata, args.project))

