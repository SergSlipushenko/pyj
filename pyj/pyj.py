import random
import uuid
import time
from contextlib import contextmanager
import json

import six

from pyj.storage import get_storage_by_url

PENDING_PREF = 'pending'
QUEUED_PREF = 'queued'
LOCK_PREF = 'lock'
META_PREF = 'meta'


def _merge_dicts(src, patch):
    res = src.copy()
    for k in set(src) | set(patch):
        if k not in patch:
            continue
        elif (k in src and isinstance(src[k], dict)
              and isinstance(patch[k], dict)):
            res[k] = _merge_dicts(src[k], patch[k])
        elif (k in src and isinstance(src[k], list)
              and isinstance(patch[k], list)):
            res[k].extend(patch[k])
        elif k in src and patch[k] is None:
            res.pop(k, None)
        else:
            res[k] = patch[k]
    return res

@contextmanager
def get_lock(storage, key):

    locks = storage.list(prefix='/'.join((LOCK_PREF, key)))
    locked = False
    if not locks:
        ts = '%d' % int(time.time() * 1E9)
        storage.put(key='/'.join((LOCK_PREF, key, ts)), body='42')
        lock = sorted(storage.list(prefix='/'.join((LOCK_PREF, key))))[0]
        locked = lock == ts
    yield locked
    if locked:
        locks = storage.list(prefix='/'.join((LOCK_PREF, key)))
        for lock in locks:
            storage.delete(key='/'.join((LOCK_PREF, key, lock)))


class Queue(object):

    def __init__(self, store, maxqueued=0):
        if isinstance(store, six.string_types):
            self.storage = get_storage_by_url(store)
        else:
            self.storage = store
        self.maxqueued = maxqueued

    def get(self, block=False, forced=True):
        while True:
            pending = self.storage.list(prefix=PENDING_PREF)
            is_not_ready = (
                not pending or
                0 < self.maxqueued <= len(self.storage.list(prefix=QUEUED_PREF))
            )
            if is_not_ready:
                if block:
                    time.sleep(1)
                    continue
                else:
                    return None, None
            jid = random.choice(pending)
            with get_lock(self.storage, jid) as locked:
                if locked:
                    job = self.storage.get(key='/'.join((PENDING_PREF, jid)))
                    if not forced:
                        self.storage.put('/'.join((QUEUED_PREF, jid)), body=job)
                    self.storage.delete('/'.join((PENDING_PREF, jid)))
                    return jid, job

    def put(self, job, jid=None):
        jid = jid or uuid.uuid4().hex
        self.storage.put(
            key='/'.join((PENDING_PREF, jid)),
            body=job
        )
        if jid:
            self.storage.delete('/'.join((QUEUED_PREF, jid)))
        return jid

    def get_pending(self):
        return {jid: self.storage.get(key='/'.join((PENDING_PREF, jid)))
                for jid in self.storage.list(prefix=PENDING_PREF)}

    def get_queued(self):
        return {jid: self.storage.get(key='/'.join((QUEUED_PREF, jid)))
                for jid in self.storage.list(prefix=QUEUED_PREF)}

    def qsize(self):
        return len(self.storage.list(prefix=PENDING_PREF))

    def empty(self):
        return self.qsize() == 0

    def delete(self, jid):
        self.storage.delete(key='/'.join((PENDING_PREF, jid)))
        self.storage.delete(key='/'.join((QUEUED_PREF, jid)))

    def drop(self):
        for jid in self.storage.list(prefix=PENDING_PREF):
            self.storage.delete(key='/'.join((PENDING_PREF, jid)))
        for jid in self.storage.list(prefix=QUEUED_PREF):
            self.storage.delete(key='/'.join((QUEUED_PREF, jid)))


class MetaStore(object):
    def __init__(self,store):
        if isinstance(store, six.string_types):
            self.storage = get_storage_by_url(store)
        else:
            self.storage = store
        self.meta_base_key = '/'.join((META_PREF, 'base'))
        self.meta_updates_key = '/'.join((META_PREF, 'updates'))

    def drop(self):
        self.storage.put(Key=self.meta_base_key, Body='')
        all_updates = self._list_updates()
        for ts in all_updates:
            self.storage.delete_object(key='%s/%s' % (self.meta_updates_key, ts))

    def get(self):
        meta, _, _ = self._get()
        return meta

    def update(self, patch, squash_if_needed=False):
        key = '%s/%s' % (self.meta_updates_key, int(time.time() * 1E9))
        self.storage.put(key, body=json.dumps(patch))
        if squash_if_needed and len(self._list_updates()) > 42:
            self.squash_updates()

    def squash_updates(self):
        meta, updates, _ = self._get()
        if not updates:
            return
        latest_update_ts = updates[-1][0]
        serialized = json.dumps((latest_update_ts, meta))
        self.storage.put(self.meta_base_key, body=serialized)
        for _ts, _ in updates:
            self.storage.delete(key='%s/%s' % (self.meta_updates_key, _ts))

    def _get_base(self):
        raw_base = self.storage.get(self.meta_base_key, default='')
        return json.loads(raw_base) if raw_base else (None, {})

    def _list_updates(self):
        return self.storage.list(self.meta_updates_key)

    def _get_updates(self):
        upd_keys = self._list_updates()
        timestamps = []
        updates = {}
        for key in upd_keys:
            upd_ts = key.split('/')[-1]
            raw_update = self.storage.get('%s/%s' % (self.meta_updates_key, key), default='')
            upd = json.loads(raw_update) if raw_update else {}
            updates[upd_ts] = upd
            timestamps.append(upd_ts)
        return [(t, updates[t]) for t in sorted(timestamps)]

    def _get(self):
        updates = self._get_updates()
        ts, meta = self._get_base()
        meta = self._apply_updates(meta, updates, ts=ts)
        return meta, updates, ts

    def _apply_updates(self, meta, updates, ts=None):
        for upd_ts, patch in updates:
            if (ts is not None) and (upd_ts < ts):
                continue
            meta = _merge_dicts(meta, patch=patch)
        return meta
