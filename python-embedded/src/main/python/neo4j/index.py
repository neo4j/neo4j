
from _backend import extends, Index

#
# Pythonification of the index API
#

class NodeIndexManager(object):

    def __init__(self, db):
        self._index = db.index()
        
    def create(self, name):
        return self._index.forNodes(name)
        
    def get(self, name):
        if self._index.existsForNodes(name):
            return self._index.forNodes(name)
        raise ValueError("No node index named %s exists." % name)
    
class RelationshipIndexManager(object):

    def __init__(self, db):
        self._index = db.index()
        
    def create(self, name):
        return self._index.forRelationships(name)
        
    def get(self, name):
        if self._index.existsForRelationships(name):
            return self._index.forRelationships(name)
        raise ValueError("No relationship index named %s exists." % name)    

        
class IndexColumn(object):

    def __init__(self, idx, key):
        self._idx = idx
        self.key = key

    def __setitem__(self, value, obj):
        return self._idx.add(obj, self.key, value)

    def __getitem__(self, value):
        return IndexHitsProxy(self._idx, self.key, value)
        
    def __delitem__(self, item):
        self._idx.remove(item, self.key)
        
class IndexHitsProxy(object):

    def __init__(self, idx, key, value):
        self._idx = idx
        self.key = key
        self.value = value

    def __iter__(self):
        return self._idx.get(self.key, self.value).iterator()
        
    def __delitem__(self, item):
        self._idx.remove(item, self.key, self.value)

class Index(extends(Index)):

    def __getitem__(self, key):
        return IndexColumn(self,key)
        
    def __delitem__(self, item):
        self.remove(item)
        
    
