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

from _backend import *

from util import rethrow_current_exception_as, PythonicIterator, CountablePythonicIterator
from json import dumps

#
# Pythonification of the core API
#

GraphDatabase = extends(GraphDatabaseService)
# Notice: Further pythinification of this class is located in __init__.py
def __new__(GraphDatabase, resourceUri, **settings):
    config = HashMap()
    for key in settings:
        value = settings[key]
        if isinstance(value, str):
            config.put(key, value)
    jpype.java.lang.System.setProperty("neo4j.ext.udc.source", "neo4py")

    return EmbeddedGraphDatabase(resourceUri, config)
    

class _Direction(extends(Direction)):
    
    def __repr__(self):
        return self.name().lower()

    def __getattr__(self, attr):
        return DirectionalType(rel_type(attr), self)

# Give the user references to the 
# actual direction instances.
BOTH = ANY = Direction.ANY = Direction.BOTH
INCOMING = Direction.INCOMING
OUTGOING = Direction.OUTGOING


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
        
    # Backwards compat
    rels = relationships
    
    def __str__(self):
        return 'Node[{0}]'.format(self.id)


class RelationshipProxy(extends(RelationshipProxy)):
    
    @property
    def start(self): return self.getStartNode()
    
    @property
    def end(self):   return self.getEndNode()
    
    def other_node(self, node):
        return self.end if self.start.id == node.id else self.start
        
    def __str__(self):
        return 'Relationship[{0},{1}]'.format(self.id, self.type)

class PropertyContainer(extends(PropertyContainer)):
    def __getitem__(self, key):
        v = self.get_property(key)
        if v != None: 
            return v
        
        raise KeyError("No property with key #{key}.")
            
    def __setitem__(self, key, value):
        self.set_property(key, value)
            
    def __delitem__(self, key):
        try:
            return self.removeProperty(key)
        except:
            rethrow_current_exception_as(KeyError)
            
    def get_property(self, key, default=None):
        try:
            v = from_java(self.getProperty(key, None))
            return v if v != None else default
        except:
            rethrow_current_exception_as(Exception)
    
    def set_property(self, key, value):
        try:
            if value is None:
                self.__delitem__(key)
            else:
                self.setProperty(key, to_java(value))
        except:
            rethrow_current_exception_as(Exception)
  
  
    # Backwards compat
    get = get_property
    set = set_property
  
    def items(self):
        for k in self.keys():
            yield k, self[k]

    def keys(self):
        for k in self.getPropertyKeys():
            yield k

    def values(self):
        for k, v in self.items():
            yield v
            
    def has_key(self, key):
        return self.hasProperty(key)
        
    def to_dict(self):
        out = {}
        for k,v in self.items():
            out[k] = v
        return out
        
    def __repr__(self):
        return ''.join([self.__str__(), dumps(self.to_dict())])
    

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

    def __init__(self, node, t):
        self.__node = node
        self.__type = t

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
            return CountablePythonicIterator(it)
        return relationships
        
    __iter__ = relationships(Direction.ANY)
    incoming = property(relationships(Direction.INCOMING))
    outgoing = property(relationships(Direction.OUTGOING))
    del relationships # (temporary helper) from the body of this class

    @property
    def single(self):
        return self.__iter__().single
        
    def __len__(self):
        return self.__iter__().__len__()
        
    def create(self, t, *nodes, **properties):
        if not nodes: raise TypeError("No target node specified")
        rels = []
        node = self.__node; 
        
        if type(t) in strings:
            t = rel_type(t)
        
        for other in nodes:
            rels.append( node.createRelationshipTo(other, t) )
        for rel in rels:
            for key, val in properties.items():
                rel[key] = val
        if len(rels) == 1: return rels[0]
        return rels

    def __call__(self, *nodes, **properties):
        return self.create(self.__type, *nodes, **properties)


class IterableWrapper(extends(IterableWrapper)):
        
    def __iter__(self):
        it = self.iterator()
        while it.hasNext():
            yield it.next()
            
    def __len__(self):
        return PythonicIterator(self.__iter__()).__len__()
            
    @property
    def single(self):
        return PythonicIterator(self.__iter__()).single
            
        
        
