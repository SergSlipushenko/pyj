import os

import click
import six
import yaml
import json
import objectpath

from pyj.storage import get_storage_by_url
from pyj.pyj import Queue, MetaStore


def _get_store(url=None):
    return get_storage_by_url(
        url or os.environ.get('PYJ_STORE_URL', 'moto://test'))


@click.group()
def cli():
    pass


@click.command()
@click.argument('job', default='')
@click.option('--file', default=None)
@click.option('--job_id', default=None)
def put(job_id, file, job):
    store = _get_store()
    store.init()
    q = Queue(store)
    if file:
        with open(file) as f:
            for line in f.readlines():
                line = line.strip()
                if line and not line.startswith('#'):
                    print(q.put(line))
    elif job:
        print(q.put(job, jid=job_id))
    else:
        return


@click.command()
@click.option('-v', is_flag=True)
@click.option('--force', is_flag=True)
@click.option('--wait', is_flag=True)
def get(v, force, wait):
    q = Queue(_get_store())
    jid, job = q.get(forced=force, block=wait)
    if jid is None:
        return
    if v:
        print('\t'.join((jid, job)))
    else:
        print(job)


@click.command()
def left():
    q = Queue(_get_store())
    print(q.qsize())


@click.command()
@click.option('-v', is_flag=True)
def pending(v):
    for jid, job in six.iteritems(Queue(_get_store()).get_pending()):
        if v:
            print('%s\t%s' % (jid, job))
        else:
            print(job)


@click.command()
@click.option('-v', is_flag=True)
def queued(v):
    for jid, job in six.iteritems(Queue(_get_store()).get_queued()):
        if v:
            print('\t'.join((jid, job)))
        else:
            print(job)


@click.command()
def init():
    _get_store().init()


@click.command()
def drop():
    Queue(_get_store()).drop()


@click.command()
@click.option('-q', default=None)
def meta_get(q):
    meta = MetaStore(_get_store()).get()
    if q:
        print(json.dumps(objectpath.Tree(meta).execute(q), indent=4))
    else:
        print(json.dumps(meta, indent=4))


@click.argument('update', default='')
@click.option('--file', default=None)
@click.command()
def meta_upd(update, file):
    metastore = MetaStore(_get_store())
    if update:
        metastore.update(yaml.safe_load(update), squash_if_needed=True)
    elif file:
        with open(file) as f:
            metastore.update(yaml.safe_load(f), squash_if_needed=True)


cli.add_command(put)
cli.add_command(get)
cli.add_command(left)
cli.add_command(pending)
cli.add_command(queued)
cli.add_command(init)
cli.add_command(drop)
cli.add_command(meta_get)
cli.add_command(meta_upd)

if __name__ == '__main__':
    cli()
