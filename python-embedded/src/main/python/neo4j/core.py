from _backend import *

#
# Pythonification of the core API
#


GraphDatabase = extends(GraphDatabaseService)
def __new__(GraphDatabase, resourceUri, **settings):
    config = HashMap()
    for key in settings:
        value = settings[key]
        if isinstance(value, str):
            config.put(key, value)
    return EmbeddedGraphDatabase(resourceUri, config)

# Hack: We need this reference
# so that we can set BOTH, INCOMING, 
# and OUTGOING references on the direction
# class created below.
_javaDirection = Direction

class Direction(extends(Direction)):
    
    def __repr__(self):
        return self.name().lower()

    def __getattr__(self, attr):
        return DirectionalType(rel_type(attr), self)

# Give the user references to the 
# actual direction instances.
Direction.BOTH = _javaDirection.BOTH
Direction.ANY = _javaDirection.BOTH
Direction.INCOMING = _javaDirection.INCOMING
Direction.OUTGOING = _javaDirection.OUTGOING
del _javaDirection

class DirectionalType(object):
    def __init__(self, reltype, direction):
        self.type = reltype
        self.dir = direction
    def __repr__(self):
        return "%r.%s" % (self.dir, self.type.name())


class NodeProxy(extends(NodeProxy)):
    def __getattr__(self, attr):
        return NodeRelationships(self, rel_type(attr))
    
    @property
    def relationships(self):
        return NodeRelationships(self, None)

class RelationshipProxy(extends(RelationshipProxy)):
    
    @property
    def start(self): return self.getStartNode()
    
    @property
    def end(self):   return self.getEndNode()

class PropertyContainer(extends(PropertyContainer)):
    def __getitem__(self, key):
        return from_java(self.getProperty(key))
    def __setitem__(self, key, value):
        self.setProperty(key, to_java(value))
  
    def items(self):
        for k in self.getPropertyKeys():
            yield k, self[k]

    def keys(self):
        for k in self.getPropertyKeys():
            yield k

    def values(self):
        for k, v in self.items():
            yield v
            
class Transaction(extends(Transaction)):
    def __enter__(self):
        return self
    def __exit__(self, exc, *stuff):
        try:
            if exc:
                self.failure()
            else:
                self.success()
        finally:
            self.finish()
            
            
class NodeRelationships(object):
    ''' Handles relationships of some
    given type on a single node.
    
    Allows creating and iterating through
    relationships.
    '''

    def __init__(self, node, rel_type):
        self.__node = node
        self.__type = rel_type

    def __repr__(self):
        if self.__type is None:
            return "%r.[All relationships]" % (self.__node)
        return "%r.%s" % (self.__node,self.__type.name())

    def relationships(direction):
        def relationships(self):
            if self.__type is not None:
                it = self.__node.getRelationships(self.__type, direction).iterator()
            else:
                it = self.__node.getRelationships(direction).iterator()
            while it.hasNext(): yield it.next()
        return relationships
    __iter__ = relationships(Direction.ANY)
    incoming = property(relationships(Direction.INCOMING))
    outgoing = property(relationships(Direction.OUTGOING))
    del relationships # (temporary helper) from the body of this class

    def __call__(self, *nodes, **properties):
        if not nodes: raise TypeError("No target node specified")
        rels = []
        node = self.__node; type = self.__type
        for other in nodes:
            rels.append( node.createRelationshipTo(other, type) )
        for rel in rels:
            for key, val in properties.items():
                rel[key] = val
        if len(rels) == 1: return rels[0]
        return rels
        

class IterableWrapper(extends(IterableWrapper)):
    
    def __iter__(self): 
        return self.iterator()
