PARQUET_FILE="s3://my-awesome-bucket/test.parquet"
PARQUET_FILE="test.parquet"

python upload_era5_to_s3.py \
    s3://era5-pds/2022/05/data/precipitation_amount_1hour_Accumulation.nc \
    $PARQUET_FILE
