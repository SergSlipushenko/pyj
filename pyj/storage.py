import boto3
from botocore.client import ClientError
S3 = None


def get_storage_by_url(url):
    if url.startswith('s3://'):
        bucket = url[len('s3://'):].split('/', 1)[0]
        prefix = url[len('s3://' + bucket):].strip('/')
        return S3Storage(bucket=bucket, prefix=prefix)
    if url.startswith('moto://'):
        bucket = url[len('moto://'):].split('/', 1)[0]
        prefix = url[len('moto://' + bucket):].strip('/')
        return S3Storage(
            bucket=bucket, prefix=prefix,
            region_name='us-west-1',
            endpoint_url='http://localhost:5000',)


class S3Storage(object):

    def __init__(self, **kwargs):
        self.bucket = kwargs.pop('bucket')
        self.prefix = kwargs.pop('prefix', '')
        self.s3 = boto3.client(service_name='s3', **kwargs)

    def _append_prefix(self, s):
        return '/'.join((self.prefix, s)).strip('/')

    def init(self):
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.s3.create_bucket(Bucket=self.bucket)
            else:
                raise e

    def drop(self):
        for key in self.list():
            self.delete(key)

    def list(self, prefix=''):
        prefix = self._append_prefix(prefix)
        return [
            o['Key'][len(prefix):].strip('/') for o in
            self.s3.list_objects(Bucket=self.bucket,
                                 Prefix=prefix).get('Contents', [])]

    def get(self, key, default=None):
        key = self._append_prefix(key)
        try:
            return self.s3.get_object(
                Bucket=self.bucket,
                Key=key
            )['Body'].read().decode("utf-8")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return default
            else:
                raise e

    def put(self, key, body):
        key = self._append_prefix(key)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)

    def delete(self, key):
        key = self._append_prefix(key)
        self.s3.delete_object(Bucket=self.bucket, Key=key)
