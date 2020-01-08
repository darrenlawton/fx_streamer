import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import boto3
from botocore.exceptions import ClientError

# archiving to glacier: https://aws.amazon.com/blogs/aws/archive-s3-to-glacier/
# folders: https://docs.aws.amazon.com/AmazonS3/latest/user-guide/using-folders.html

# Per: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False

    return True


def get_object_name(key, fx_pair, stream_date):
    return "/".join([fx_pair,stream_date.year,stream_date.month,stream_date.day,key])


