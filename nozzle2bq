# -*- coding: utf-8 -*-
"""Nozzle BigQuery Data Load"""

import json
import logging
import base64
import pandas as pd

from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import pubsub

from tqdm.auto import tqdm

_LOG = logging.getLogger( "locomotive_ranking" )
_LOG.setLevel(logging.DEBUG)

"""
requirements.txt file:

google-cloud-bigquery
google-cloud-pubsub
pandas
"""



""" Config Data"""

# Credentials

# TODO: (Developer) Upload service account json as service_account.json
SERVICE_ACCOUNT_DICT = "service_account.json"

SERVICE_ACCOUNT_SCOPES = ['https://www.googleapis.com/auth/bigquery', 
                          'https://www.googleapis.com/auth/drive',
                          'https://www.googleapis.com/auth/pubsub']



# Project Information
GCP_PROJECT = "nozzle-ranking-data"
BQ_DATASET = "client_ranking_data"
GCP_TOPIC = "nozzle-trigger-run"


# Bigquery Information
NOZZLE_QUERY = """
WITH
-- find the latest versioned keyword data
-- this can also be used to pin a query to an older version, good for static reports
latest_keyword_source_versions AS (
  SELECT keyword_source_id, MAX(keyword_source_version_id) AS keyword_source_version_id
  FROM `nozzledata.{nozzle_dataset}.keywords`
  GROUP BY keyword_source_id
),

-- filter keyword groups first to avoid accidentally aggregating
-- on multiple rows based on keyword groupings
filtered_keywords AS (
  SELECT
    keyword_id,
    ANY_VALUE(phrase_id) AS phrase_id,
    ANY_VALUE(phrase) AS phrase,
    ANY_VALUE(locale_id) AS locale_id,
    ANY_VALUE(device) AS device,
    ANY_VALUE(engine) AS engine,
    ANY_VALUE(language) AS language,
    ANY_VALUE(location_id) AS location_id,
    ANY_VALUE(location_type) AS location_type,
    ANY_VALUE(location) AS location,
    ANY_VALUE(country) AS country,
    ANY_VALUE(ad_words_criteria_id) AS ad_words_criteria_id,
    ANY_VALUE(grp) AS keyword_group,
  FROM `nozzledata.{nozzle_dataset}.keywords`  
  JOIN latest_keyword_source_versions USING (keyword_source_id, keyword_source_version_id)
  JOIN UNNEST( ARRAY_CONCAT(`groups`, ["all"]) ) AS grp
  
  WHERE phrase IS NOT NULL

  GROUP BY keyword_id
),

distinct_keyword_ids AS (
  SELECT
    keyword_id,
    phrase,
    keyword_group
  FROM filtered_keywords
  GROUP BY keyword_id, phrase, keyword_group
),

-- filter rankings by keyword ids + search level fields
latest_filtered_rankings AS (
  SELECT AS VALUE
    ARRAY_AGG(t ORDER BY inserted_at DESC LIMIT 1)[OFFSET(0)]
  FROM `nozzledata.{nozzle_dataset}.rankings` t
  WHERE requested = '{nozzle_requested_date}'
  -- Below is no longer used.  We are requesting a specific date above.
  -- WHERE CAST(requested AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CAST(requested AS DATE) < CURRENT_DATE()
  GROUP BY ranking_id
),

-- This is no longer used.
latest_requested AS (
  SELECT MAX(requested) AS requested
  FROM latest_filtered_rankings
  WHERE requested = '{nozzle_requested_date}'
),

-- apply any result level filters here, set group by, and get all metrics necessary for later calculations
latest_filtered_rankings_results AS (
  SELECT
    keyword_id,
    requested,
    result.url.domain AS result__url__domain,
    result.url.url AS result__url__url,
    result.url.url_id,
    result.rank,
    result.paid_adjusted_rank,
    result.nozzle_metrics.click_through_rate,
    result.nozzle_metrics.estimated_traffic,
    result.nozzle_metrics.ppc_value,
    result.measurements.pixels_from_top,
    result.measurements.pixel_height * result.measurements.pixel_width AS result_pixels_total,
    result.measurements.percentage_of_viewport,
    result.measurements.percentage_of_dom,
    
  FROM latest_filtered_rankings
  
  JOIN filtered_keywords USING (keyword_id)
  -- JOIN latest_requested USING (requested) # Note: latest_filtered_rankings is now filtered to a specific date.
  JOIN UNNEST(results) AS result
  
  WHERE (result.paid IS NULL OR result.paid.is_paid=FALSE) 
  AND result.url.domain = '{nozzle_domain}'
  
),

latest_filtered_rankings_results_withnulls AS (
  SELECT
    *
  FROM filtered_keywords
  LEFT JOIN latest_filtered_rankings_results USING (keyword_id)
),


-- calculate min metrics per keyword_id, group_by, and requested
per_serp_metrics AS (
  SELECT
    IFNULL(requested, (SELECT MAX(requested) FROM latest_filtered_rankings_results_withnulls) )  AS requested,
    keyword_id,
    keyword_group,
    phrase,
    device,
    engine,
    language,
    location,
    country,
    result__url__domain,
    result__url__url,
    
    IFNULL(MIN(rank), 101) AS top_rank,
    IFNULL(MIN(paid_adjusted_rank), 101) AS top_paid_adjusted_rank,
    IFNULL(MIN(pixels_from_top), 30000) AS top_pixels_from_top,
    IFNULL(SUM(click_through_rate), 0) AS click_through_rate,
    IFNULL(SUM(percentage_of_viewport), 0) AS percentage_of_viewport,
    IFNULL(SUM(percentage_of_dom), 0) AS percentage_of_dom,


  FROM latest_filtered_rankings_results_withnulls
  
  GROUP BY  keyword_id, 
            keyword_group,
            requested,
            result__url__domain,
            phrase,
            device,
            engine,
            language,
            location,
            country,
            result__url__url
)


SELECT *
FROM per_serp_metrics
"""


""" Functions"""

def build_client(service_account_data=None,
                 scopes=None,
                 project=None, 
                 client_type="bigquery"):
    """Builds bigquery or pubsub client

    Parameters
    ----------
    service_account_data : str or dict
        Your service account data as filename or dict (default is
        value of global SERVICE_ACCOUNT_DICT)
    scopes : list, optional
        GCP execution scopes (default is
        value of SERVICE_ACCOUNT_SCOPES)
    project : str, optional
        The GCP project
    client_type : str, optional
        Either `bigquery` or `pubsub` (default is `bigquery`)

    Returns
    -------
    google.cloud client
        Either `bigquery` or `pubsub` client
    """

    project = project or GCP_PROJECT
    service_account_data = service_account_data or SERVICE_ACCOUNT_DICT
    scopes = scopes or SERVICE_ACCOUNT_SCOPES

    if isinstance(service_account_data, dict):
        credentials = service_account.\
                      Credentials.\
                      from_service_account_info(service_account_data, \
                                                scopes=scopes)

    elif isinstance(service_account_data, str):
        credentials = service_account.\
                      Credentials.\
                      from_service_account_file(service_account_data, \
                                                scopes=scopes)
              
    else:
        raise AttributeError("Only file location (str) or json (dict) are " \
                              "valid for --service_account_data.")


    if client_type == "bigquery":
        client = bigquery.Client(credentials=credentials,
                                 project=project)
    elif client_type == "pubsub":
        client = pubsub.PublisherClient(credentials=credentials)     
    else:
        raise AttributeError("Only `bigquery` and `pubsub` are supported"\
                             " values for --client_type")         
    
    _LOG.info('{} client built successfully.'.format(client_type))

    return client


def get_client_data(client=None):
    """Gets client data from config table.

    Parameters
    ----------
    client : google.cloud.bigquery client, optional
        Bigquery client

    Returns
    -------
    pandas.DataFrame
        Dataframe with client data
    """

    # Construct a BigQuery client object.
    client = client or build_client(client_type="bigquery")

    query_string = "SELECT * FROM "\
                   "`{project}.{dataset}.config`"\
                   " LIMIT 100".format(project=GCP_PROJECT,
                                       dataset=BQ_DATASET)

    df = client.query(query_string).to_dataframe()

    _LOG.info(df.info())

    return df


def maybe_create_tables(df, client=None):
    """Validates that tables exist for all clients

    Parameters
    ----------
    df : pandas.DataFrame
        Nozzle client data (client, domain, nozzle_dataset)
    client : google.cloud.bigquery client, optional
        Bigquery client

    Returns
    -------
    success : boolean
        Validation of tables succeeded.
    errors : list
        List of errors in format {'table': ..., 'error': ...}
    """

    if df is None or not isinstance(df, pd.DataFrame):
        raise AttributeError('A valid --df `pd.DataFrame` must be provided.')

    client = client or build_client(client_type="bigquery")
    table_id_format = "{project}.{dataset}.{table}"

    schema = [
      bigquery.SchemaField('requested', 'TIMESTAMP', mode='REQUIRED'),
      bigquery.SchemaField('keyword_id', 'INTEGER', mode='REQUIRED'),
      bigquery.SchemaField('keyword_group', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('phrase', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('device', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('engine', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('language', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('location', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('country', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('result__url__domain', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('result__url__url', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('top_rank', 'INTEGER', mode='NULLABLE'),
      bigquery.SchemaField('top_paid_adjusted_rank', 'INTEGER', mode='NULLABLE'),
      bigquery.SchemaField('top_pixels_from_top', 'INTEGER', mode='NULLABLE'),
      bigquery.SchemaField('click_through_rate', 'FLOAT', mode='NULLABLE'),
      bigquery.SchemaField('percentage_of_viewport', 'FLOAT', mode='NULLABLE'),
      bigquery.SchemaField('percentage_of_dom', 'FLOAT', mode='NULLABLE')
    ]

    errors = []

    for row in df.itertuples():

        nozzle_dataset = row.nozzle_dataset.strip().lower()

        table_id = table_id_format.format(project=GCP_PROJECT,
                                          dataset=BQ_DATASET,
                                          table=nozzle_dataset
                                          )

        # Try to get table.
        try:
            client.get_table(table_id)  # Get Table
            _LOG.info("Table `{}` exists.".format(table_id))
        
        # If not found, create table.
        except NotFound:

            _LOG.info("Creating table `{}`.".format(table_id))

            try:
                table = bigquery.Table(table_id, schema=schema)
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                     # name of column to use for partitioning
                    field="requested",
                    expiration_ms=None,
                )
                table = client.create_table(table)  # Create table
                _LOG.info(
                    "Created table `{}.{}.{}`.".format(table.project, 
                                                       table.dataset_id, 
                                                       table.table_id)
                )

            except Exception as e:
                _LOG.error(str(e))
                error_info = {'table': table_id, 'error': str(e)}
                errors.append(error_info)

    return len(errors) == 0, errors


def get_new_requested_dates(nozzle_dataset, client=None):
    """Returns list of new dates for Nozzle data

    Parameters
    ----------
    nozzle_dataset : str
        Nozzle dataset name
    client : google.cloud.bigquery client, optional
        Bigquery client

    Returns
    -------
    list
        List of new dates

    """
 
    client = client or build_client(client_type="bigquery")

    n_query = "SELECT DISTINCT(requested) "\
              "FROM `nozzledata.{}.rankings`".format(nozzle_dataset)
    l_query = "SELECT DISTINCT(requested) FROM "\
              "`{project}.{dataset}.{table}`"\
              .format(project=GCP_PROJECT,
                      dataset=BQ_DATASET,
                      table=nozzle_dataset)

    n_requested = client.query(n_query).to_dataframe().requested.tolist()
    l_requested = client.query(l_query).to_dataframe().requested.tolist()

    # Return new dates
    return [str(t).strip() for t in n_requested if t not in l_requested]
    


def update_nozzle_ranking(df, query, client=None):
    """Tries to run the given query on all datasets from
    provided client DataFame

    Parameters
    ----------
    df : pandas.DataFrame
        Nozzle client data (client, domain, nozzle_dataset)
    query : str
        The query to run on BigQuery
    client : google.cloud.bigquery client, optional
        Bigquery client

    Returns
    -------
    success : boolean
        Validation of tables succeeded.
    errors : list
        List of errors in format {'table': ..., 'error': ...}
    """

    if not query or not isinstance(query, str):
        raise AttributeError('A valid --query `str` must be provided.')

    if df is None or not isinstance(df, pd.DataFrame):
        raise AttributeError('A valid --df `pd.DataFrame` must be provided.')

    client = client or build_client(client_type="bigquery")

    errors = []

    for row in df.itertuples():

        table_id_format = "{project}.{dataset}.{table}"
        nozzle_dataset  = row.nozzle_dataset.lower().strip()
        dest_table_id   = table_id_format.format(project=GCP_PROJECT,
                                                 dataset=BQ_DATASET,
                                                 table=nozzle_dataset
                                                 )
        
        nozzle_domain   = row.domain.lower().strip()

        _LOG.info("Loading data into {}".format(dest_table_id))

        new_requested_dates = get_new_requested_dates(nozzle_dataset, 
                                                      client=client)

        for requested_date in tqdm(new_requested_dates):

            try:

                job_config = bigquery.\
                             QueryJobConfig(destination=dest_table_id,
                                            write_disposition = bigquery.\
                                                                WriteDisposition.\
                                                                WRITE_APPEND,
                                            project=GCP_PROJECT)

                sql = query.format(nozzle_dataset=nozzle_dataset,
                                   nozzle_domain=nozzle_domain,
                                   nozzle_requested_date=requested_date)

                # Start the query, passing in the extra configuration.
                query_job = client.query(sql, job_config=job_config)
                query_job.result()  # Wait for the job to complete.

                _LOG.info("Data loaded for table {}".format(dest_table_id))

            except Exception as e:

                _LOG.error(str(e))
                error_info = {'table': dest_table_id, 'error': str(e)}
                errors.append(error_info)


    return len(errors) == 0, errors


def publish_message(message, client=None):
    """Publishes a message to pubsub topic (GCP_TOPIC)

    Parameters
    ----------
    message : str
        Message to send
    client : google.cloud.pubsub client, optional
        PubSub client

    Returns
    -------
    google.cloud.pubsub client
        Result
    """

    bytes_message = bytes(message, encoding='utf-8')

    publisher_client = client or build_client(client_type="pubsub")
    
    topic_id = "projects/{project_id}/topics/{topic}"\
              .format(project_id=GCP_PROJECT,
                      topic=GCP_TOPIC)

    result = publisher_client.publish(topic_id, bytes_message)

    return result


def get_message(event):
    """Gets message from Cloud Function event

    Parameters
    ----------
    event : dict
        A GCP Cloud Function event.

    Returns
    -------
    str
        The message or empy string if no data
  
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_data = json.loads(pubsub_message)
    
    if isinstance(pubsub_data, dict) and 'job' in pubsub_data:
        return pubsub_data['job']
    
    return ""


def run_client_messages(df, client=None):
    """Sends messages for all supplied clients in DataFrame

    Parameters
    ----------
    df : pandas.DataFrame
        Nozzle client data (client, domain, nozzle_dataset)
    client : google.cloud.pubsub client, optional
        PubSub client

    Returns
    -------
    success : boolean
        Validation of tables succeeded.
    errors : list
        List of errors in format {'table': ..., 'error': ...}
  
    """

    if df is None or not isinstance(df, pd.DataFrame):
        raise AttributeError('A valid --df `pd.DataFrame` must be provided.')

    publisher_client = client or build_client(client_type="pubsub")

    results = []
    errors = []

    for row in tqdm(df.itertuples()):

        nozzle_dataset = row.nozzle_dataset.lower().strip()

        message   = json.dumps({'job': 'run {}'.format(nozzle_dataset)})

        pub_job = publish_message(message, client=publisher_client)

        result = pub_job.result() 

        if result:
            results.append(result)
        else:
            error_info = {'table': nozzle_dataset, 'error': \
                          'Error publishing message: {}'.format(message)}
            errors.append(error_info)

   
    _LOG.info(results)

    return len(errors) == 0, errors


def run_sql_updates(df, client=None):
    """Runs table validation and sql queries across all clients in
    DataFrame

    Parameters
    ----------
    df : pandas.DataFrame
        Nozzle client data (client, domain, nozzle_dataset)
    client : google.cloud.bigquery client, optional
        Bigquery client

    Returns
    -------
    success : boolean
        Validation of tables succeeded.
    errors : list
        List of errors in format {'table': ..., 'error': ...}
    """

    if df is None or not isinstance(df, pd.DataFrame):
        raise AttributeError('A valid --df `pd.DataFrame` must be provided.')

    errors = []
    success = True

    client = client or build_client(client_type="bigquery")

    success, run_errors = maybe_create_tables(df, client=client)

    if success:

        _LOG.info("Table validation succeeded.")

        success, run_errors = update_nozzle_ranking(df, 
                                                    NOZZLE_QUERY,
                                                    client=client) 
        if success:
            _LOG.info("Data load succeeded.")
        else:
            _LOG.error('run_notebook failed updating tables.')
            errors.extend(run_errors)

    else:
        _LOG.error('run_notebook failed creating tables.')
        errors.extend(run_errors)

    
    return success, errors


def log_errors(errors):
  """Simple error logging function"""

  for err in errors:
    _LOG.error("{}: {}".format(*err.values()))

_LOG.setLevel(logging.DEBUG)



# Call this from Cloud Functions
def run_job(event=None, context=None):
    """Main function.  Also can be cloud function entrypoint.

    Parameters
    ----------
    event : dict
        Cloud function event information
    context : dict
        Cloud function context information

    Returns
    -------
    bool
        True if ran without errors

    """

    client = build_client(client_type="bigquery")
    clients_df = get_client_data(client=client)

    success = True
    errors = []

    if event:
        #_LOG.setLevel(logging.ERROR)

        data = get_message(event)

        if data == 'run all':
            success, run_errors = run_client_messages(clients_df)
            errors.extend(run_errors)

        elif 'run' in data:
            nozzle_dataset = data.replace('run', '').strip().lower()
            clients_df = clients_df[clients_df.nozzle_dataset.str.contains(nozzle_dataset)]
            success, run_errors = run_sql_updates(clients_df, client=client)
            errors.extend(run_errors)

        else:
            err = 'Invalid pubsub data: {}'.format(data)
            _LOG.error(err)
            success = False
            errors.append({'table': None, 'error': err})       

    else:
        # This is a notebook.
        success, run_errors = run_sql_updates(clients_df, client=client)

    if success:
        _LOG.info('Run Succeeded!')
        return True
    else:
        _LOG.error('Run Failed!')
        log_errors(errors)
        return False



""" Tests"""

def t1():
    # Run Job from Notebook:
    return run_job()

def t2():
    # Run Job as Cloud Function (one)
    message   = json.dumps({'job': 'run locomotive_locomotive'})
    event = {'data': base64.b64encode(bytes(message, encoding='utf-8'))}
    return run_job(event=event)

def t3():
    # Run Job as Cloud Function (all)
    message   = json.dumps({'job': 'run all'})
    event = {'data': base64.b64encode(bytes(message, encoding='utf-8'))}
    return run_job(event=event)

