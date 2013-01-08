# Copyright (c) 2002-2013 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from _backend import extends, Index, IndexHits, to_java
from neo4j.util import rethrow_current_exception_as, PythonicIterator

#
# Pythonification of the index API
#

class NodeIndexManager(object):

    def __init__(self, db):
        self._index = db.index()
        
    def create(self, name, **config):
        return self._index.forNodes(name, to_java(config))
        
    def get(self, name):
        if self.exists(name):
            return self._index.forNodes(name)
        raise ValueError("No node index named %s exists." % name)
        
    def exists(self, name):
        return self._index.existsForNodes(name)
    
class RelationshipIndexManager(object):

    def __init__(self, db):
        self._index = db.index()
        
    def create(self, name, **config):
        return self._index.forRelationships(name, to_java(config))
        
    def get(self, name):
        if self.exists(name):
            return self._index.forRelationships(name)
        raise ValueError("No relationship index named %s exists." % name)
        
    def exists(self, name):
        return self._index.existsForRelationships(name)

        
class IndexColumn(object):

    def __init__(self, idx, key):
        self._idx = idx
        self.key = key

    def __setitem__(self, value, obj):
        return self._idx.add(obj, self.key, value)

    def __getitem__(self, value):
        return IndexCell(self._idx, self.key, value)
        
    def __delitem__(self, item):
        self._idx.remove(item, self.key)
        
class IndexCell(object):
    ''' This class supports the
    del idx['key']['value'][item]
    semantics.
    
    For everything else, it delegates
    to IndexHits, with some wrapping
    code.
    
    The reason is that we don't want to
    bother executing an unnecessary index
    search when we do deletes.
    '''
  
    def __init__(self, idx, key, value):
        self._idx = idx
        self.key = key
        self.value = value

    def __len__(self):
        return self._get_hits(False).__len__()

    def __iter__(self):
        return PythonicIterator(self._get_hits().iterator())

    def __getitem__(self, item):
        return self._get_hits().__getitem__(item)
        
    def __delitem__(self, item):
        self._idx.remove(item, self.key, self.value)
        
    @property
    def single(self):
        return self.__iter__().single
        
    def close(self):
        if hasattr(self, '_cached_hits'):
            self._cached_hits.close()
            del self._cached_hits
        
    def _get_hits(self, close=True):
        if close:
            self.close()
        
        if not hasattr(self, '_cached_hits'):
            self._cached_hits = self._idx.get(self.key, self.value)
        return self._cached_hits


class IndexHits(extends(IndexHits)):

    def __len__(self):
        return self.size() if self.size() > 0 else 0

    def __getitem__(self, item):
        if isinstance(item, slice):
            # Memory-hogging slicing impl
            return list(self).__getitem__(item)
        elif isinstance(item, int) and item >= 0:
            for i in range(item):
                self.next()
            return self.next()


class Index(extends(Index)):

    def __getitem__(self, key):
        return IndexColumn(self,key)
        
    def __delitem__(self, item):
        self.remove(item)
        
    
