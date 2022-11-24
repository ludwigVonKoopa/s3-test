import argparse
import os

import boto3
import s3fs
import xarray as xr

import processing


def usage():
    """Parse the options provided on the command line.

    Returns:
        argparse.Namespace: The parameters provided on the command line.
    """

    parser = argparse.ArgumentParser(
        description=(
            "tool to convert netcdf file to parquet. "
            "download netcdf file, convert it to parquet format then upload it. "
            "if --no-download is specified, it will stream the dataset in the processing"
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("dataset", help="dataset to convert to parquet")

    parser.add_argument("parquet_file",
        help=(
            "pathname for the parquet file. "
            "If it start with 's3://', it will upload it on s3 server"
        )
    )

    parser.add_argument("--no-download", action="store_true",
        help="don't download netcdf file before converting it to parquet file"
    )

    return parser.parse_args()


def split_bucket_and_filename(filename):
    """extract bucket name and basename from a filename

    ex : split_bucket_and_filename("s3://my-bucket/folder1/test.parquet")
        => return ("my-bucket/", "folder1/test.parquet")

    ex : split_bucket_and_filename("my-bucket/folder1/test.parquet")
        => return ("my-bucket/", "folder1/test.parquet")

    Parameters
    ----------
    filemane : str
        name of the file

    Returns
    -------
    tuple
        bucket name, basename
    """

    # boto3 don't want the "s3://" in the bucket name
    if filename.startswith("s3://"):
        filename_corrected = filename[5:]
    else:
        filename_corrected = filename

    bucket_name, *splitted_filename = filename_corrected.split("/")
    filename = "/".join(splitted_filename)
    return bucket_name, filename


if __name__ == "__main__":

    args = usage()

    parquet_file = args.parquet_file

    # if the file is hosted on s3
    if args.dataset.startswith("s3://"):

        # if streaming mode
        if args.no_download:
            fs = s3fs.S3FileSystem()
            fileObj = fs.open(args.dataset)
        else:
            # download file and store it locally
            print(f"downloading netcdf file {args.dataset} ..")

            s3 = boto3.client('s3')
            bucket_name, filename = split_bucket_and_filename(args.dataset)
            local_filename = os.path.basename(filename)

            # if already exist don't download again
            if os.path.exists(local_filename):
                print(f"local netcdf file {local_filename} already exist!")
            else:
                s3.download_file(bucket_name, filename, local_filename)
            print(f"downloading netcdf {local_filename} done")

            fileObj = local_filename

        ds = xr.open_dataset(fileObj)
    else:
        ds = xr.open_dataset(args.dataset)

    print(ds)
    print()

    # correction of the netcdf file
    # => deleting unecessary coordinates, applying standart names
    ds = ds.drop_vars("time1_bounds").rename(
        {"precipitation_amount_1hour_Accumulation": "precipitation", "time1": "time"}
    )

    # computing h3 indexes
    ds = processing.build_h3_indexes(ds, levels=[0, 1, 2, 3], h3_type="int")
    print(ds)
    print()


    # if parquet file specified ends with s3
    if args.parquet_file.startswith("s3://"):
        bucket_name, filename = split_bucket_and_filename(args.parquet_file)
        local_filename = os.path.basename(filename)

        print(f"converting netcdf file to parquet '{local_filename}' ...")
        processing.ds_to_parquet(ds, local_filename, dim_time="time")
        print("converting netcdf file to parquet done")


        print(f"uploading parquet file '{local_filename}' to {args.parquet_file} ...")
        s3_client = boto3.client('s3')
        response = s3_client.upload_file(local_filename, bucket_name, filename)
        print(f"uploading parquet file '{local_filename}' done")

    else:
        print(f"converting netcdf file to parquet '{args.parquet_file}' ...")
        processing.ds_to_parquet(ds, args.parquet_file, dim_time="time")
        print("converting netcdf file to parquet done")
