import json
import boto3
from botocore.client import ClientError
import random
import uuid
import time


BUCKET = 'test'
PENDING_PREF = 'pending/'
QUEUED_PREF = 'queued/'
FINISHED_PREF = 'finished/'
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


def _create_bucket_if_not_exists():
    try:
        S3.head_bucket(Bucket=BUCKET)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            S3.create_bucket(Bucket=BUCKET)
        else:
            raise e


def _get_object(Bucket, Key, defaul=None):
    try:
        return S3.get_object(Bucket=Bucket,
                             Key=Key)['Body'].read().decode("utf-8")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return defaul
        else:
            raise e


def backfill_jobs(jobs, s3=None):
    s3 = s3 or S3 or _get_s3()
    for j in jobs:
        s3.put_object(Bucket=BUCKET,
                      Key='%s%s' % (PENDING_PREF, uuid.uuid4().hex),
                      Body=j)


def report_result(jid, exit_code):
    S3.put_object(Bucket=BUCKET,
                  Key='%s%s' % (FINISHED_PREF, jid),
                  Body=json.dumps({'exit_code': exit_code,
                                   'job': get_job(jid)}))
    S3.delete_object(Bucket=BUCKET, Key='%s%s' % (QUEUED_PREF, jid))


def get_result(jid, s3=None):
    res = _get_object(Bucket=BUCKET,
                      Key='%s%s' % (FINISHED_PREF, jid))
    if res:
        return json.loads(res)


def get_job(jid):
    job = _get_object(Bucket=BUCKET, Key=''.join((PENDING_PREF, jid)))
    if job:
        return job
    job = _get_object(Bucket=BUCKET, Key=''.join((QUEUED_PREF, jid)))
    if job:
        return job
    result = _get_object(Bucket=BUCKET, Key=''.join((FINISHED_PREF, jid)))
    if result:
        return result['job']


def get_pending():
    return _list_keys(prefix=PENDING_PREF)


def get_queued():
    return _list_keys(prefix=QUEUED_PREF)


def get_finished():
    return _list_keys(prefix=FINISHED_PREF)


def pick_job():
    while True:
        pending = _list_keys(prefix=PENDING_PREF)
        if not pending:
            return None, None
        jid = random.choice(pending)
        if aquire_lock(jid):
            job = get_job(jid)
            move_job_to_queued(jid)
            release_lock(jid)
            return jid, job


def move_job_to_queued(jid):
    S3.copy_object(Bucket=BUCKET, Key='%s%s' % (QUEUED_PREF, jid),
                   CopySource=dict(Bucket=BUCKET,
                                   Key='%s%s' % (PENDING_PREF, jid)))
    S3.delete_object(Bucket=BUCKET, Key='%s%s' % (PENDING_PREF, jid))


def delete_job(jid):
    S3.delete_object(Bucket=BUCKET, Key='%s%s' % (QUEUED_PREF, jid))
    S3.delete_object(Bucket=BUCKET, Key='%s%s' % (PENDING_PREF, jid))
    S3.delete_object(Bucket=BUCKET, Key='%s%s' % (FINISHED_PREF, jid))


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

    @click.group()
    def cli():
        pass

    @click.command()
    @click.argument('jobs', nargs=-1)
    def put(jobs):
        _create_bucket_if_not_exists()
        backfill_jobs(jobs)

    @click.command()
    @click.argument('job_id', nargs=1)
    @click.argument('exit_code', nargs=1)
    def report(job_id, exit_code):
        report_result(job_id, exit_code)


    @click.command()
    @click.option('-v', is_flag=True)
    def get(v):
        jid, job = pick_job()
        if jid is None:
            return
        if v:
            print('\t'.join((jid, job)))
        else:
            print(job)

    @click.command()
    @click.option('-v', is_flag=True)
    def pending(v):
        for jid in get_pending():
            if v:
                print('\t'.join((jid, get_job(jid))))
            else:
                print(get_job(jid))

    @click.command()
    @click.option('-v', is_flag=True)
    def queued(v):
        for jid in get_queued():
            if v:
                print('\t'.join((jid, get_job(jid))))
            else:
                print(get_job(jid))

    @click.command()
    @click.option('--job', is_flag=True)
    @click.option('--exit_code', is_flag=True)
    @click.option('-a', is_flag=True)
    def finished(job, exit_code, a):
        for jid in get_finished():
            result = get_result(jid)
            res = [jid]
            if a or exit_code:
                res.append(result['exit_code'])
            if a or job:
                res.append(result['job'])
            print('\t'.join(res))


    @click.command()
    def drop():
        for jid in get_pending() + get_queued() + get_finished():
            delete_job(jid)


    cli.add_command(put)
    cli.add_command(get)
    cli.add_command(pending)
    cli.add_command(queued)
    cli.add_command(finished)
    cli.add_command(report)
    cli.add_command(drop)

    S3 = _get_s3()

    cli()
