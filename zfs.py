#!/usr/bin/env python3

import unittest
import argparse
import sys
import subprocess
import re
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile, generate_latest, REGISTRY
from prometheus_client.metrics import MetricWrapperBase, Lock

pool_states = [
    "degraded",
    "faulted",
    "offline",
    "online",
    "removed",
    "unavail",
]


def level(s: str) -> int:
    level = 0
    for c in s:
        if c == '\t':
            continue
        elif c == ' ':
            level += 1
        else:
            break
    return level//2


def resolve_trace_to_labels(trace):
    labels = {}

    if trace[-1].startswith('/'):
        labels['disk'] = trace.pop()

    labels['pool'] = '/'.join(list(dict.fromkeys(trace)))
    return labels


class Enum(MetricWrapperBase):
    _type = 'stateset'

    def __init__(self,
                 name,
                 documentation,
                 labelnames=(),
                 namespace='',
                 subsystem='',
                 unit='',
                 registry=REGISTRY,
                 labelvalues=None,
                 states=None,
                 enum_name=None,
                 ):
        super(Enum, self).__init__(
            name=name,
            documentation=documentation,
            labelnames=labelnames,
            namespace=namespace,
            subsystem=subsystem,
            unit=unit,
            registry=registry,
            labelvalues=labelvalues,
        )
        if enum_name is None:
            enum_name = name
        if enum_name in labelnames:
            raise ValueError(
                'Overlapping labels for Enum metric: %s' % (name,))
        if not states:
            raise ValueError(
                'No states provided for Enum metric: %s' % (name,))
        self._kwargs['states'] = self._states = states
        self._kwargs['enum_name'] = self._enum_name = enum_name

    def _metric_init(self):
        self._value = 0
        self._lock = Lock()

    def state(self, state):
        """Set enum metric state."""
        self._raise_if_not_observable()
        with self._lock:
            self._value = self._states.index(state)

    def _child_samples(self):
        with self._lock:
            x = [
                ('', {self._enum_name: s}, 1 if i == self._value else 0,)
                for i, s
                in enumerate(self._states)
            ]
            return x


class ZFS(object):
    def __init__(self, registry, snapshots_ignore=[]):
        self._snapshots_ignore = [
            re.compile(x) for x in sum(snapshots_ignore, [])
        ]
        self._metrics = {
            'disk': {},
            'pool': {},
            'snapshots': {},
        }

        self._metrics['pool']['state'] = Enum(
            name='status',
            namespace='zfs',
            subsystem='pool',
            documentation='Status of ZFS pool',
            labelnames=['pool'],
            states=pool_states,
            registry=registry,
            enum_name='state',
        )
        self._metrics['disk']['state'] = Enum(
            name='status',
            namespace='zfs',
            subsystem='pool_disk',
            documentation='Status of a single disk in a ZFS pool',
            labelnames=['pool', 'disk'],
            states=pool_states,
            registry=registry,
            enum_name='state',
        )

        self._metrics['pool']['errors'] = Gauge(
            name='errors_total',
            namespace='zfs',
            subsystem='pool',
            documentation='Total count of ZFS pool errors',
            labelnames=['pool', 'type'],
            registry=registry,
        )
        self._metrics['disk']['errors'] = Gauge(
            name='errors_total',
            namespace='zfs',
            subsystem='pool_disk',
            documentation='Total count of ZFS disk errors',
            labelnames=['pool', 'disk', 'type'],
            registry=registry,
        )

        self._metrics['snapshots']['count'] = Gauge(
            name='count',
            namespace='zfs',
            subsystem='snapshot',
            documentation='Count of existing ZFS snapshots',
            labelnames=['dataset'],
            registry=registry,
        )
        self._metrics['snapshots']['last'] = Gauge(
            name='last_unixtime',
            namespace='zfs',
            subsystem='snapshot',
            documentation='Time of last ZFS snapshot',
            labelnames=['dataset'],
            registry=registry,
        )
        self._metrics['snapshots']['sizes'] = Gauge(
            name='disk_used',
            namespace='zfs',
            subsystem='snapshot',
            documentation='Disk space used by all snapshots.',
            labelnames=['dataset'],
            registry=registry,
        )

    def run(self):
        self.zpool_status()
        self.zfs_snapshots()

    def zpool(self, *args, check=True):
        """Wrapper around invoking the zpool binary.
        """
        return subprocess.run(
            ['zpool', *args], stdout=subprocess.PIPE, check=check
        ).stdout.decode('utf-8')

    def zfs(self, *args, check=True):
        """Wrapper around invoking the zpool binary.
        """
        return subprocess.run(
            ['zfs', *args], stdout=subprocess.PIPE, check=check
        ).stdout.decode('utf-8')

    def parse_zpool_status(self, data):
        active = False
        trace = []
        keys = None
        for line in data.split('\n'):
            fields = line.split()

            # skip empty fields
            if len(fields) == 0:
                continue

            if len(fields) > 1 and fields[0] == "pool:":
                trace = [fields[1]]
                active = False
                keys = None
                continue

            # handle start and stop
            if not active:
                if line.startswith('config:'):
                    active = True
                continue
            else:
                if line.startswith('errors:'):
                    active = False
                    continue

            if len(fields) < 1:
                continue

            lvl = level(line)
            trace = trace[0:1+lvl]
            trace.append(fields[0])

            if len(fields) < 5:
                continue

            if keys is None:
                keys = {}
                for pos, field in enumerate(fields):
                    keys[field.lower()] = pos
                continue

            metric_labels = resolve_trace_to_labels(trace)
            metric_type = 'pool'
            if 'disk' in metric_labels:
                metric_type = 'disk'

            # set state
            self._metrics[metric_type]['state'].labels(
                **metric_labels,
            ).state(fields[keys['state']].lower())

            # set errors
            error_mapping = {'read': None, 'write': None, 'cksum': 'checksum'}
            for k, v in error_mapping.items():
                if v is None:
                    v = k
                labels = metric_labels
                labels['type'] = v
                self._metrics[metric_type]['errors'].labels(
                    **metric_labels,
                ).inc(int(fields[keys[k]]))

    def zpool_status(self):
        self.parse_zpool_status(self.zpool('status', '-Pp'))

    def zfs_snapshots(self):
        self.parse_zfs_snapshots(
            self.zfs(
                'list', '-H', '-p',
                '-t', 'snapshot',
                '-o', 'name,creation,used',
            ))

    def parse_zfs_snapshots(self, data):
        values = {}

        for line in data.split('\n'):
            fields = line.split()
            if len(fields) < 3:
                continue

            name = fields[0].split('@')[0]

            # check if dataset is ignored
            match = False
            for regex in self._snapshots_ignore:
                if regex.match(name):
                    match = True
                    break
            if match:
                continue

            if name in values:
                values[name]['count'] += 1
                values[name]['sizes'] += int(fields[2])
                ts = int(fields[1])
                if values[name]['last'] < ts:
                    values[name]['last'] = ts

            else:
                values[name] = {
                    'last': int(fields[1]),
                    'count': 1,
                    'sizes': int(fields[2]),
                }

        for name in values:
            for key in self._metrics['snapshots']:
                self._metrics['snapshots'][key].labels(
                    name,
                ).set(values[name][key])


class TestZFS(unittest.TestCase):
    def zpool_status(self, name):
        file_prefix = "testdata/zpool-status-%s" % name

        with open(file_prefix + '.out', 'r', encoding='utf-8') as f:
            input = f.read()
        with open(
                file_prefix + '.prom',
                'r',
                encoding='utf-8',
        ) as f:
            expected = f.read()

        registry = CollectorRegistry()
        ZFS(registry).parse_zpool_status(input)
        actual = generate_latest(registry=registry).decode('utf-8')

        actual = '\n'.join(filter(
            lambda x: not x.startswith('#'),
            actual.split('\n'),
        ))

        self.maxDiff = 409600
        self.assertEqual(actual, expected)

    def test_zpool_status_raidz_cache(self):
        self.zpool_status('raidz-cache')

    def test_zpool_status_three_pools(self):
        self.zpool_status('three-pools')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-S', '--snapshots-ignore',
                        action='append', nargs='*', default=[])
    parser.add_argument('-d', '--destination-path',
                        default='./zfs.prom')
    args = parser.parse_args(sys.argv[1:])

    registry = CollectorRegistry()
    ZFS(registry, snapshots_ignore=args.snapshots_ignore).run()
    write_to_textfile(args.destination_path, registry)


if __name__ == '__main__':
    main()
