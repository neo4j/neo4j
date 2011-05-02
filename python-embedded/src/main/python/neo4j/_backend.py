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
    class MetaClass(type):
        def __new__(Class, name, bases, body):
            if bases == ():
                return type.__new__(Class, name, (object,), body)
            else:
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
    jpype.startJVM(jpype.getDefaultJVMPath(),
                   '-Djava.class.path=' + os.getenv('CLASSPATH','.'))
    graphdb = jpype.JPackage('org.neo4j.graphdb')
    kernel  = jpype.JPackage('org.neo4j.kernel')
    Direction = graphdb.Direction
    PropertyContainer = graphdb.PropertyContainer
    Transaction = graphdb.Transaction
    GraphDatabaseService = graphdb.GraphDatabaseService
    Node = graphdb.Node
    Relationship = graphdb.Relationship
    EmbeddedGraphDatabase = kernel.EmbeddedGraphDatabase
    HashMap = jpype.JPackage('java.util').HashMap
    rel_type = graphdb.DynamicRelationshipType.withName
    del graphdb, kernel # to get a consistent namespace

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
else:
    from org.neo4j.kernel import EmbeddedGraphDatabase
    from org.neo4j.graphdb import Direction, DynamicRelationshipType,\
        PropertyContainer, Transaction, GraphDatabaseService, Node, Relationship
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
    pass


class PropertyContainer(extends(PropertyContainer)):
    def __getitem__(self, key):
        return from_java(self.getProperty(key))
    def __setitem__(self, key, value):
        self.setProperty(key, to_java(value))

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
