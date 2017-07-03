import boto3
from botocore.client import ClientError
import random
import uuid
import time


BUCKET = 'test'
PENDING_PREF = 'pending/'
LOCK_PREF = 'lock/'
S3 = None


def _get_s3():
    return boto3.client(
        service_name='s3',
        region_name='us-west-1',
        endpoint_url='http://localhost:5000',
    )


def _get_ts():
    return '%d' % int(time.time()*1000000)


def _list_keys(prefix='', s3=None):
    s3 = s3 or S3 or _get_s3()
    return [
        o['Key'][len(prefix):] for o in
        s3.list_objects(Bucket=BUCKET,
                        Prefix=prefix).get('Contents', [])]


def _create_bucket_if_not_exists(s3=None):
    s3 = s3 or S3 or _get_s3()
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3.create_bucket(Bucket=BUCKET)
        else:
            raise e


def put_jobs(jobs, s3=None):
    s3 = s3 or S3 or _get_s3()
    for j in jobs:
        s3.put_object(Bucket=BUCKET,
                      Key='%s%s' % (PENDING_PREF, uuid.uuid4().hex),
                      Body=j)


def get_job(jid, s3=None):
    s3 = s3 or S3 or _get_s3()
    return s3.get_object(
        Bucket=BUCKET, Key=''.join((PENDING_PREF, jid))
    )['Body'].read().decode("utf-8")


def pick_job():
    while True:
        pending = _list_keys(prefix=PENDING_PREF)
        if not pending:
            return
        jid = random.choice(pending)
        if aquire_lock(jid):
            job = get_job(jid)
            delete_job(jid)
            _list_keys(prefix=PENDING_PREF)
            release_lock(jid)
            return jid, job


def delete_job(jid, s3=None):
    s3 = s3 or S3 or _get_s3()
    s3.delete_object(Bucket=BUCKET, Key='%s%s' % (PENDING_PREF, jid))


def aquire_lock(jid, s3=None):
    s3 = s3 or S3 or _get_s3()
    locks = _list_keys(prefix='%s%s/' % (LOCK_PREF, jid))
    if locks:
        return False
    ts = _get_ts()
    s3.put_object(Bucket=BUCKET, Key='%s%s/%s' % (LOCK_PREF, jid, ts), Body='42')
    lock = sorted(_list_keys(prefix='%s%s/' % (LOCK_PREF, jid)))[0]
    if lock == ts:
        return True


def release_lock(jid, s3=None):
    s3 = s3 or S3 or _get_s3()
    locks = _list_keys(prefix='%s%s/' % (LOCK_PREF, jid))
    for lock in locks:
        s3.delete_object(Bucket=BUCKET, Key='%s%s/%s' % (LOCK_PREF, jid, lock))


if __name__ == '__main__':
    import click
    import os


    @click.group()
    def cli():
        pass

    @click.command()
    @click.argument('jobs', nargs=-1)
    def put(jobs):
        _create_bucket_if_not_exists()
        put_jobs(jobs)


    @click.command()
    def get():
        jid, job = pick_job()
        print jid, job

    @click.command()
    def ls():
        pending = _list_keys(prefix=PENDING_PREF)
        for jid in pending:
            print('%s => %s' % (jid, get_job(jid)))

    @click.command()
    def drop():
        pending = _list_keys(prefix=PENDING_PREF)
        for jid in pending:
            delete_job(jid)

    cli.add_command(put)
    cli.add_command(get)
    cli.add_command(ls)
    cli.add_command(drop)

    S3 = _get_s3()

    cli()
