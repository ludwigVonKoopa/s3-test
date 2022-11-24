# s3-test# installation

Please install [anaconda](https://www.anaconda.com/products/distribution#linux) for easy python environment managing. You can follow [this link](https://docs.anaconda.com/anaconda/install/linux/#installation)

# instruction

To use this tool, please update your `~/.aws/credentials` file with your aws access key and secret access key.

Example of credential file :

```bash
>>> cat ~/.aws/credentials
[default]
aws_access_key_id=ABCDEFGHIJKLMNOP
aws_secret_access_key=uvwxyz1234567890+abcd
```

It will be used by python s3 lib to connect to your account to retrieve the netcdf file and push it to your bucket

# start

to perform the processing :
* activate the python environment `conda activate s3_preprocessing`
* use `make run`

if you want to test the processing with s3 uploading, please update the `upload_to_s3.sh` file with your s3 bucket name.
Else, it will transform era5 file into a parquet file locally.
