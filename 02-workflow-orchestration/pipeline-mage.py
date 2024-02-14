
""""
DATA LOADER BLOC
""""
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float 
    }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']


    dfs = []

    for i in ['10','11','12']:
        full_url = url + f"green_tripdata_2020-{i}.csv.gz"
        df = pd.read_csv(full_url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        dfs.append(df)

    data = pd.concat(dfs)

    print(data.shape)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


""""
TRANSFORMER BLOC
""""

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    print(data["VendorID"].unique())
    data.columns = [col.lower().replace('id', '_id') for col in data.columns]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    print(data.shape)
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero distance'
    assert "vendor_id" in output.columns, 'vendor_id column doesnt exist'


""""
DATA EXPORTER TO PostgreSQL BLOC
""""
SELECT * FROM {{ df_1 }}

""""
DATA EXPORTER TO GCS BLOC
""""

import pyarrow as pa 
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

bucket_name = 'mage-zoom-omar'
project_id = 'booming-primer-413709'
table_name = 'green_taxi'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(data, **kwargs) -> None:
    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path= root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem = gcs
    )
