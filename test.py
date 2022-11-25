import pytest
import s3fs
from upload_era5_to_s3 import split_bucket_and_filename


def test_s3_credentials():
    "test if the s3 credentials have been set up"
    fs = s3fs.S3FileSystem()

    # should not raise PermissionError
    fileObj = fs.open("s3://era5-pds/2022/05/data/precipitation_amount_1hour_Accumulation.nc")

# split_bucket_and_filename

def test_split_bucket_and_filename():

    b, f = split_bucket_and_filename("my-awesome-bucket/file.nc")
    assert b, f == ("my-awesome-bucket", "file.nc")

    b, f = split_bucket_and_filename("my-awesome-bucket/folder1/file.nc")
    assert b, f == ("my-awesome-bucket", "folder1/file.nc")

    b, f = split_bucket_and_filename("s3://my-awesome-bucket/folder1/file.nc")
    assert b, f == ("my-awesome-bucket", "folder1/file.nc")
