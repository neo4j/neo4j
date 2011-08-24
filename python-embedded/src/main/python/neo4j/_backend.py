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
     
    class ParentClass(type):
        def __new__(Class, name, bases, body):
            if bases == ():
                return type.__new__(Class, name, (object,), body)
            else:
                for key, value in body.items():
                    if key not in ('__module__','__new__'):
                        if hasattr(CLASS, key):
                            setattr(CLASS, "_super__%s" % key, getattr(CLASS, key))
                        setattr(CLASS, key, value)
                return type(name, (object,), body)
    
    return ParentClass(getattr(CLASS,'__name__','Class'),(),{})

if sys.version_info >= (3,):
    class Type(type):
        def __new__(Type, name, bases, body):
            if '__metaclass__' in body:
                return body['__metaclass__'](name, bases, body)
            else:
                return type.__new__(Type, name, bases, body)
    Object = Type('Object', (object,), {})
    del Type # to get a consistent namespace regardless of version
    strings = str,
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
    NotFoundException = graphdb.NotFoundException
    Index = graphdb.index.Index
    
    rel_type = graphdb.DynamicRelationshipType.withName
    
    kernel  = jpype.JPackage('org.neo4j.kernel')
    EmbeddedGraphDatabase = kernel.EmbeddedGraphDatabase
    Traversal = kernel.Traversal
    TraversalDescriptionImpl = kernel.impl.traversal.TraversalDescriptionImpl
    TraverserImpl = kernel.impl.traversal.TraverserImpl
    Uniqueness = kernel.Uniqueness
    NodeProxy = kernel.impl.core.NodeProxy
    RelationshipProxy = kernel.impl.core.RelationshipProxy
    
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
    from org.neo4j.kernel.impl.core import NodeProxy, RelationshipProxy
    from org.neo4j.kernel import Uniqueness
    from org.neo4j.kernel.impl.traversal import TraversalDescriptionImpl, TraverserImpl
    from org.neo4j.graphdb import Direction, DynamicRelationshipType,\
        PropertyContainer, Transaction, GraphDatabaseService, Node, Relationship, Path, NotFoundException
    from org.neo4j.graphdb.traversal import Evaluation, Evaluator
    from org.neo4j.graphdb.index import Index
    from org.neo4j.helpers.collection import IterableWrapper
    from java.util import HashMap
    rel_type = DynamicRelationshipType.withName

    def from_java(value):
        return value
    def to_java(value):
        return value
        
    def implements(*interfaces):
        return interfaces

