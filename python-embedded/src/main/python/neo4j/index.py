
from _backend import extends, Index, IndexHits

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
        return self._get_hits().iterator()

    def __getitem__(self, item):
        return self._get_hits().__getitem__(item)
        
    def __delitem__(self, item):
        self._idx.remove(item, self.key, self.value)
        
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
            i = 0
            needles = range(*item.indices(len(self)))
            needle = needles.pop(0)
            for hit in self:
                if i == needle:    
                    yield hit
                    
                    if len(needles) == 0:
                        return
                        
                    needle = needles.pop(0)
                i += 1


class Index(extends(Index)):

    def __getitem__(self, key):
        return IndexColumn(self,key)
        
    def __delitem__(self, item):
        self.remove(item)
        
    
