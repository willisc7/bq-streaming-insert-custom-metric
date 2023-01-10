#!/usr/bin/env python

import argparse
import datetime
import pprint
import random
import time

from google.cloud import bigquery
import googleapiclient.discovery


def get_bq_number_req_data_point():
    client = bigquery.Client()
    
    #query_job = client.query(
    #    """
    #    SELECT *
    #    FROM `region-us-east1.INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_PROJECT`
    #    ORDER BY start_timestamp DESC
    #    LIMIT 10"""
    #)

    query_job = client.query(
        """
        SELECT start_timestamp,SUM(total_requests) AS total_requests,SUM(total_rows) AS total_rows,SUM(total_input_bytes) AS total_input_bytes
        FROM `region-us.INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_PROJECT`
        GROUP BY start_timestamp
        ORDER BY 1 DESC
        LIMIT 1"""
    )

    #query_job = client.query(
    #    """
    #    SELECT project_id,dataset_id,table_id,SUM(total_rows) AS num_rows,SUM(total_input_bytes) AS num_bytes,SUM(total_requests) AS num_requests
    #    FROM `region-us-east1.INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_PROJECT`
    #    GROUP BY 1, 2, 3
    #    ORDER BY num_bytes DESC
    #    LIMIT 10"""
    #)

    results = query_job.result() 

    for row in results:
        return row.total_rows

def format_rfc3339(datetime_instance=None):
    """Formats a datetime per RFC 3339.
    :param datetime_instance: Datetime instanec to format, defaults to utcnow
    """
    return datetime_instance.isoformat("T") + "Z"


def get_start_time():
    # Return now- 5 minutes
    start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
    return format_rfc3339(start_time)


def get_now_rfc3339():
    # Return now
    return format_rfc3339(datetime.datetime.utcnow())


def create_custom_metric(client, project_id,
                         custom_metric_type, metric_kind):
    """Create custom metric descriptor"""
    metrics_descriptor = {
        "type": custom_metric_type,
        "labels": [
            {
                "key": "total_rows",
                "valueType": "STRING",
                "description": "num of total_rows"
            }
        ],
        "metricKind": metric_kind,
        "valueType": "INT64",
        "unit": "items",
        "description": "BQ_Information_Schema_Metric.",
        "displayName": "BQ_Information_Schema_Metric"
    }

    return client.projects().metricDescriptors().create(
        name=project_id, body=metrics_descriptor).execute()


def delete_metric_descriptor(
        client, custom_metric_name):
    """Delete a custom metric descriptor."""
    client.projects().metricDescriptors().delete(
        name=custom_metric_name).execute()


def get_custom_metric(client, project_id, custom_metric_type):
    """Retrieve the custom metric we created"""
    request = client.projects().metricDescriptors().list(
        name=project_id,
        filter='metric.type=starts_with("{}")'.format(custom_metric_type))
    response = request.execute()
    print('ListCustomMetrics response:')
    pprint.pprint(response)
    try:
        return response['metricDescriptors']
    except KeyError:
        return None

def get_custom_data_point():
    """Dummy method to return a mock measurement for demonstration purposes.
    Returns a random number between 0 and 10"""
    length = random.randint(0, 10)
    print("reporting timeseries value {}".format(str(length)))
    return length


# [START write_timeseries]
def write_timeseries_value(client, project_resource,
                           custom_metric_type, project_id, metric_kind):
    # Specify a new data point for the time series.
    now = get_now_rfc3339()
    timeseries_data = {
        "metric": {
            "type": custom_metric_type,
            "labels": {
                "environment": "STAGING"
            }
        },
        "resource": {
            "type": 'global',
            "labels": {
                'project_id': project_id
            }
        },
        "points": [
            {
                "interval": {
                    "startTime": now,
                    "endTime": now
                },
                "value": {
                    "int64Value": get_bq_number_req_data_point()
                }
            }
        ]
    }

    request = client.projects().timeSeries().create(
        name=project_resource, body={"timeSeries": [timeseries_data]})
    request.execute()
# [END write_timeseries]


def read_timeseries(client, project_resource, custom_metric_type):
    """Reads all of the CUSTOM_METRICS that we have written between START_TIME
    and END_TIME
    :param project_resource: Resource of the project to read the timeseries
                             from.
    :param custom_metric_name: The name of the timeseries we want to read.
    """
    request = client.projects().timeSeries().list(
        name=project_resource,
        filter='metric.type="{0}"'.format(custom_metric_type),
        pageSize=3,
        interval_startTime=get_start_time(),
        interval_endTime=get_now_rfc3339())
    response = request.execute()
    return response


def main(project_id):
    # This is the namespace for all custom metrics
    CUSTOM_METRIC_DOMAIN = "custom.googleapis.com"
    # This is our specific metric name
    CUSTOM_METRIC_TYPE = "{}/bigquery_stats".format(CUSTOM_METRIC_DOMAIN)
    DATASET_ID = "ds_streaming"
    METRIC_KIND = "GAUGE"

    project_resource = "projects/{0}".format(project_id)
    client = googleapiclient.discovery.build('monitoring', 'v3')
    create_custom_metric(client, project_resource,
                         CUSTOM_METRIC_TYPE, METRIC_KIND)
    custom_metric = None
    while not custom_metric:
        # wait until it's created
        time.sleep(1)
        custom_metric = get_custom_metric(
            client, project_resource, CUSTOM_METRIC_TYPE)

    write_timeseries_value(client, project_resource,
                           CUSTOM_METRIC_TYPE, project_id, METRIC_KIND)
    # Sometimes on new metric descriptors, writes have a delay in being read
    # back. 3 seconds should be enough to make sure our read call picks up the
    # write
    time.sleep(3)
    timeseries = read_timeseries(client, project_resource, CUSTOM_METRIC_TYPE)
    print('read_timeseries response:\n{}'.format(pprint.pformat(timeseries)))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--project_id', help='Project ID you want to access.', required=True)

    args = parser.parse_args()
    main(args.project_id)

# [END all]