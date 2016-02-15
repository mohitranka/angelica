
"""
Angelica is a data access layer for a document store.

The implementation is built in order to have a caching layer on top of a Riak
Document store, or to change the underlying implementation of the data store
from a single place.

It is named after the Biblioteca Angelica in Rome
http://en.wikipedia.org/wiki/Biblioteca_Angelica
http://www.romeing.it/wp-content/uploads/2013/01/Biblioteca-Angelica.jpg
"""
import riak
import json

import pylibmc

#TODO - Move caching to decorator


class Angelica(object):

    """
    >>> a = Angelica()

    >>> a.create('b3',u'k3', 4)
    True

    >>> a.get('b3', 'k3')
    4

    >>> a.exists('b3', 'k3')
    True

    >>> a.exists('b3', 'k4')
    False

    >>> a.delete('b3', 'k4')
    False

    >>> a.delete('b3', u'k3')
    True

    >>> a.exists('b3', u'k3')
    False

    >>> a.get('b3', 'k4')

    >>> a.get('b3', 'k3')

    >>> a.create('b3','k3', 4)
    True

    >>> a.get('b3', u'k3')
    4
    """
    def __init__(
        self,
        nodes=[
            {
                'host': '127.0.0.1',
                'pb_port': 8087
            }
        ],
        memcached_servers=['localhost:11211']
    ):
        self.rc = riak.RiakClient(protocol='pbc', nodes=nodes)
        self.mc = pylibmc.Client(servers=memcached_servers)

    def get(self, bucket, key):
        value = self.mc.get('%s/%s' % (bucket, str(key)))
        if value:
            value = json.loads(value)
        if not value:
            b = self.rc.bucket_type('default').bucket(bucket)
            robj = b.get(key)
            if robj.exists:
                value = robj.data
        return value

    def create(self, bucket, key, data, caching_timeout=0):
        self.mc.set(
            '%s/%s' % (bucket, str(key)),
            json.dumps(data),
            time=caching_timeout
        )
        b = self.rc.bucket_type('default').bucket(bucket)
        ro = b.new(key, data)
        ro.store()
        return ro.exists

    def exists(self, bucket, key):
        b = self.rc.bucket_type('default').bucket(bucket)
        robj = b.get(key)
        return robj.exists

    def delete(self, bucket, key):
        if self.exists(bucket, key):
            self.mc.delete(
                '%s/%s' % (bucket, str(key))
            )
            b = self.rc.bucket_type('default').bucket(bucket)
            ro = b.delete(key)
            return not ro.exists
        else:
            return False

if __name__ == '__main__':
    import doctest
    doctest.testmod()
