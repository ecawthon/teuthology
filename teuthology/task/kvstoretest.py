import contextlib
import logging

from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run kvstorebench

    The config should be as follows::

		  kvstorebench:
		      clients: [client list]
		      operations: <number of ops to perform>
		      entries: <number of entries to start with>
		      k: <each object has k < number of pairs < 2k>
		      keysize: <number of characters per object map key>
		      valsize: <number of characters per object map val>
		      increment: <interval to show in histogram (in ms)>
		      injection: <d to kill clients, w to have them wait> <wait time (in ms)>
		      distribution: <% of ops that should be inserts> <% updates> <% deletes> <% reads>
		      in-flight: <number of ops to have at once per client>

    example::

		  tasks:
		  - ceph:
		  - kvstorebench:
		      clients: [client list]
		      operations: 100
		      entries: 30
		      k: 2
		      keysize: 5
		      valsize: 7
		      increment: 10
		      injection: w 1000
		      distribution: 25 25 25 25
		      in-flight: 256
		  - interactive:
    """
    log.info('Beginning kvstorebench...')
    assert isinstance(config, dict), \
        "please list clients to run on"
    kvstorebench = {}
    print(str(config.get('increment',-1)))
    for role in config.get('clients', ['client.0']):
        assert isinstance(role, basestring)
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        proc = remote.run(
            args=[
                "/bin/sh", "-c",
                " ".join(['CEPH_CONF=/tmp/cephtest/ceph.conf',
                          'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
                          '/tmp/cephtest/enable-coredump',
                          '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
                          '/tmp/cephtest/archive/coverage',
                          '/tmp/cephtest/binary/usr/local/bin/kvstorebench',
                          '-k', '/tmp/cephtest/data/{role}.keyring'.format(role=role),
                          '--name', role[len(PREFIX):],
                          '--ops', str(config.get('operations', 100)),
                          '--entries', str(config.get('entries', 30)),
                          '--kval', str(config.get('k',2)),
                          '--keysize', str(config.get('keysize',10)),
                          '--valsize', str(config.get('valsize',1000)),
                          '--cache-size', str(config.get('cachesize',1000)),
                          '--cache-refresh', str(config.get('cacherefresh',20)),
                          '--inc', str(config.get('increment',10)),
                          '--inj', str(config.get('interrupt','')),
                          '-d', str(config.get('distribution','25 25 25 25')),
                          '-t', str(config.get('in-flight',256)),
                          '-r', str(config.get('rand',id_)),
                          '2>&1 | tee logfile'
                          ]),
                ],
            logger=log.getChild('kvstorebench.{id}'.format(id=id_)),
            stdin=run.PIPE,
            wait=False
            )
        kvstorebench[id_] = proc

    try:
        yield
    finally:
        log.info('joining kvstorebench')
        run.wait(kvstorebench.itervalues())
