#!/usr/bin/python3
import csv
import datetime
import flywheel
import json
import logging
import os

import transfer_log
import utils


log = logging.getLogger('grp-5_transfer_log_report')


def create_missing_session_error(session_key):
    return {
        'error': 'session {}-{} missing from flywheel'.format(session_key[0], session_key[1]),
        'path': None,
        'type': 'session',
        'resolved': False,
        'label': None,
        '_id': None
    }


def create_unexpected_session_error(session, client):
    return {
        'error': 'session in flywheel not present in transfer log',
        'path': utils.get_resolver_path(client, session),
        'type': 'session',
        'resolved': False,
        'label': session.label,
        '_id': session.id
    }


def create_output_file(error_containers, file_type, gear_context, headers=None,
                       output_filename=None, validate_transfer_log=False):
    """Creates the output file from a set of error containers, the file type
    is determined from the config value

    Args:
        error_containers (str): The container type to describe in the output file
        error_containers (list): list of containers that were tagged
        file_type (str): The file type to format the output into
        gear_context (GearContext): the gear context so that we can write out
            the file
        output_filename (str): and optional file name that can be passed
        validate_transfer_log (bool): Use malformed transfer log headers
        headers(list): list of headers to use for csv

    Returns:
        str: The filename that was used to write the report as
    """
    file_ext = 'csv' if file_type == 'csv' else 'json'
    output_filename = output_filename or 'transfer-log-report.{}'.format(file_ext)
    if not headers:
        headers = transfer_log.TRANSFER_LOG_ERROR_HEADERS if validate_transfer_log else transfer_log.CSV_HEADERS
    with gear_context.open_output(output_filename, 'w') as output_file:
        if file_type == 'json':
            json.dump(error_containers, output_file)
        elif file_type == 'csv':
            csv_dict_writer = csv.DictWriter(output_file, fieldnames=headers)
            csv_dict_writer.writeheader()
            for container in error_containers:
                csv_dict_writer.writerow(container)
        else:
            raise Exception('CRITICAL: {} is not a valid file type')
    return output_filename


def update_analysis_label(parent_type, parent_id, analysis_id, analysis_label,
                          apikey, api_url):
    """Helper function to make a request to the api without the sdk because the
    sdk doesn't support updating analysis labels

    Args:
        parent_type (str): Singularized container type
        parent_id (str): The id of the parent
        analysis_id (str): The id of the analysis
        analysis_label (str): The label that should be set for the analysis
        apikey (str): The api key for the client
        api_url (str): The url for the api

    Returns:
        dict: Api response for the request
    """
    import requests

    url = '{api_url}/{parent_name}/{parent_id}/analyses/{analysis_id}'.format(
        api_url=api_url,
        parent_name=parent_type+'s',
        parent_id=parent_id,
        analysis_id=analysis_id
    )

    headers = {
        'Authorization': 'scitran-user {}'.format(apikey),
        'Content-Type': 'application/json'
    }

    data = json.dumps({
        "label": analysis_label
    })

    raw_response = requests.put(url, headers=headers, data=data)
    return raw_response.json()


def main():
    with flywheel.GearContext() as gear_context:
        gear_context.init_logging()
        log.info(gear_context.config)
        log.info(gear_context.destination)
        analysis = gear_context.client.get_analysis(
            gear_context.destination['id']
        )
        parent = gear_context.client.get_container(analysis.parent['id'])
        parent_path = utils.get_resolver_path(gear_context.client, parent)

        # Run the metadata script
        try:
            error_df, error_count = transfer_log.main(
                gear_context, 'INFO', parent_path
            )
        except transfer_log.TransferLogException as e:
            create_output_file(e.errors, 'csv', gear_context,
                               'error-transfer-log.csv', True)
            raise e

        log.info('Writing error report')
        fname = gear_context.config.get('filename')
        error_report_path = os.path.join(gear_context.output_dir, fname)
        error_df.to_csv(error_report_path, index=False)
        log.info('Wrote error report with filename %s', error_report_path)

        # Update analysis label
        timestamp = datetime.datetime.utcnow()
        analysis_label = 'TRANSFER_ERROR_COUNT_{}_AT_{}'.format(error_count, timestamp)
        log.info(
            'Updating label of analysis=%s to %s', analysis.id, analysis_label
        )

        analysis.update({'label': analysis_label})


if __name__ == '__main__':
    main()

