# -*- mode: Python; coding: utf-8 -*-

# Copyright (c) 2002-2011 "Neo Technology,"
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

"""Neo4j backend selection for Python."""

import sys, neo4j

def extends(CLASS):
    ## This lets us extend java classes "in place",
    ## adding methods and attributes to the original
    ## java classes, using the normal python class
    ## system as a DSL.
    
    class MetaClass(type):
        def __new__(Class, name, bases, body):
            if bases == ():
                return type.__new__(Class, name, (object,), body)
            else:
                overrides = []
                for key, value in body.items():
                    if key not in ('__module__','__new__'):
                        setattr(CLASS, key, value)
                        
                return type(name, (object,), body)
    
    return MetaClass(getattr(CLASS,'__name__','Class'),(),{})

if sys.version_info >= (3,):
    class Type(type):
        def __new__(Type, name, bases, body):
            if '__metaclass__' in body:
                return body['__metaclass__'](name, bases, body)
            else:
                return type.__new__(Type, name, bases, body)
    Object = Type('Object', (object,), {})
    del Type # to get a consistent namespace regardless of version
    strings = str
    integers = int,
else:
    Object = object
    strings = str, unicode
    integers = int, long


try:
    import java
except: # this isn't jython (and doesn't have the java module)
    import jpype, os

    jvmargs = ['-Djava.class.path=' + os.getenv('CLASSPATH','.')]
    
    debug = False
    if debug:
        jvmargs = jvmargs + ['-Xdebug', '-Xnoagent', '-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8000']
        
    jpype.startJVM(jpype.getDefaultJVMPath(), *jvmargs)
    
    graphdb = jpype.JPackage('org.neo4j.graphdb')
    Direction = graphdb.Direction
    PropertyContainer = graphdb.PropertyContainer
    Transaction = graphdb.Transaction
    GraphDatabaseService = graphdb.GraphDatabaseService
    Node = graphdb.Node
    Relationship = graphdb.Relationship
    Path = graphdb.Path
    Evaluation = graphdb.traversal.Evaluation
    Evaluator = graphdb.traversal.Evaluator
    rel_type = graphdb.DynamicRelationshipType.withName
    
    kernel  = jpype.JPackage('org.neo4j.kernel')
    EmbeddedGraphDatabase = kernel.EmbeddedGraphDatabase
    TraversalDescriptionImpl = kernel.impl.traversal.TraversalDescriptionImpl
    TraverserImpl = kernel.impl.traversal.TraverserImpl
    Uniqueness = kernel.Uniqueness
    
    helpers = jpype.JPackage('org.neo4j.helpers')
    IterableWrapper = helpers.collection.IterableWrapper
    
    HashMap = jpype.JPackage('java.util').HashMap
    
    del graphdb, kernel, helpers # to get a consistent namespace

    def from_java(value):
        global from_java
        java = jpype.java.lang
        floats = (java.Double, java.Float)
        integers = (java.Long, java.Integer, java.Short, java.Byte)
        def from_java(value):
            if isinstance(value, floats):
                return value.doubleValue()
            if isinstance(value, integers):
                return value.longValue()
            return value
        return from_java(value)
    def to_java(value):
        return value
    
    def implements(*interfaces):
      class InterfaceProxy(object):
          def __new__(cls, *args, **kwargs):
              inst = super(InterfaceProxy, cls).__new__(cls, *args, **kwargs)
              inst.__init__(*args, **kwargs)
              return jpype.JProxy(interfaces, inst=inst)
      return InterfaceProxy
else:
    from org.neo4j.kernel import EmbeddedGraphDatabase, Uniqueness
    from org.neo4j.kernel.impl.traversal import TraversalDescriptionImpl, TraverserImpl
    from org.neo4j.graphdb import Direction, DynamicRelationshipType,\
        PropertyContainer, Transaction, GraphDatabaseService, Node, Relationship, Path
    from org.neo4j.graphdb.traversal import Evaluation, Evaluator
    from org.neo4j.helpers.collection import IterableWrapper
    from java.util import HashMap
    rel_type = DynamicRelationshipType.withName

    def from_java(value):
        return value
    def to_java(value):
        return value


class Direction(Object):
    class __metaclass__(type):
        INCOMING = Direction.INCOMING
        OUTGOING = Direction.OUTGOING
        ANY=BOTH = Direction.BOTH

    def __init__(self, direction):
        self.__dir = direction

    def __repr__(self):
        return self.__dir.name().lower()

    def __getattr__(self, attr):
        return DirectionalType(rel_type(attr), self.__dir)

class DirectionalType(object):
    def __init__(self, reltype, direction):
        self.__type = reltype
        self.__dir = direction
    def __repr__(self):
        return "%r.%s" % (self.__dir, self.__type.name())


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


class Node(extends(Node)):
    def __getattr__(node, attr):
        return Relationships(node, rel_type(attr))
    def __div__(self, other):
        return neo4j.BoundPathTraversal(self, other)
    __truediv__ = __div__


class Relationship(extends(Relationship)):
    
    @property
    def start(self): return self.getStartNode()
    
    @property
    def end(self):   return self.getEndNode()


class PropertyContainer(extends(PropertyContainer)):
    def __getitem__(self, key):
        return from_java(self.getProperty(key))
    def __setitem__(self, key, value):
        self.setProperty(key, to_java(value))


class Path(extends(Path)):

    @property
    def start(self): return self.startNode()
    
    @property
    def end(self):   return self.endNode()
    
    @property
    def last_relationship(self): return self.lastRelationship()
    
    def __repr__(self): return self.toString()
    
    def __len__(self):  return self.length()
    
    def __iter__(self):
        for entity in self.iterator():
            yield entity 
            

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
           
class TraverserImpl(extends(TraverserImpl)):
    
    def __iter__(self): 
        return self.iterator()
      

class IterableWrapper(extends(IterableWrapper)):
    
    def __iter__(self): 
        return self.iterator()
        
        
class Relationships(object):
    def __init__(self, node, rel_type):
        self.__node = node
        self.__type = rel_type

    def __repr__(self):
        return "%r.%s" % (self.__node,self.__rel_type.name())

    def relationships(direction):
        def relationships(self):
            it = self.__node.getRelationships(self.__type, direction).iterator()
            while it.hasNext(): yield it.next()
        return relationships
    __iter__ = relationships(Direction.BOTH)
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
        

