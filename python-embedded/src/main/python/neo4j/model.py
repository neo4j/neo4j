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

"""Neo4j domain modeling support for Python.
"""

__all__ = 'Node', 'Relationship', 'Property', 'transactional'

from neo4j._backend import Object, strings, integers
from neo4j.util import iterator

import datetime, time

class Neo4jEntity(Object):
    def __new__(Neo4jEntity, entity):
        instance = object.__new__(Neo4jEntity)
        instance.__entity = entity
        return instance
    
# BEGIN: name mangling hackery
    def _get_entity(self):
        return self.__entity
get_entity = Neo4jEntity._get_entity; del Neo4jEntity._get_entity
# END: name mangling hackery

class Factory(object):
    def __init__(self, type, args, params=None):
        self.__type = type
        self.__args = args
        if params is None: params = {}
        self.__params = params
    def __str__(self):
        return "%s(%s,%s)" % (self.__type.__name__, ', '.join(self.__args),
                              ', '.join(('%s=%s' % (k,v)) for k,v in
                                        self.__params.items()))
    def create(self, type, name):
        obj = object.__new__(self.__type)
        obj.__init__(type, name, *self.__args, **self.__params)
        return obj

@iterator
def lookup_results(Type, hits, filters):
    try:
        for node in hits:
            for predicate in filters:
                if not predicate(node): break
            else:
                yield Neo4jEntity.__new__(Type, node)
    finally:
        try:
            hits.close()
        except:
            pass

class Node(Neo4jEntity):
    __node = property(get_entity)
    class __metaclass__(type):
        def __new__(Node,name,bases,body):
            Node = type.__new__(Node,name,bases,body)
            for attr, value in body.items():
                if isinstance(value, Factory):
                    setattr(Node, attr, value.create(Node, attr))
            return Node
        def find(Node, graphdb, **query):
            lookup = []; filters = []
            for key in query:
                attribute = getattr(Node, key.split('__',1)[0])
                if attribute is None:
                    raise AttributeError("'%s' has no property matching '%s'"
                                         % (Node.__name__, key))
                attribute.query(key, query[key], lookup, filters)
            hits = Node._index(graphdb).query(" AND ".join(lookup))
            return lookup_results(Node, hits, filters)
        def _index(Node, graphdb):
            return graphdb.index().forNodes(Node.__name__)

    def __new__(Node, graphdb, **properties):
        node = Neo4jEntity.__new__(Node, graphdb.createNode())
        for key, value in properties.items():
            descr = getattr(Node, key)
            if descr:
                descr.__set__(node, value)
            else:
                raise AttributeError("Node type '%s' has no attribute '%s'"
                                     % (Node.__name__, key))
        return node

class Relationship(Neo4jEntity):
    __relationship = property(get_entity)
    class __metaclass__(type):
        def __new__(Relationship,name,bases,body):
            Relationship = type.__new__(Relationship,name,bases,body)
            for attr, value in body.items():
                if isinstance(value, Factory):
                    setattr(Relationship,attr,value.create(Relationship,attr))
            return Relationship
        def _index(Relationship, graphdb):
            return graphdb.index().forRelationships(Relationship.__name__)

    def __new__(Relationship, target, type=None, direction=None):
        # TODO: return a Factory of something else?
        return Factory(Relationship, (target, type, direction))

    def __init__(self, source, name, target, type=None, direction=None):
        pass

def transactional(method):
    def transactional(self, *args, **kwargs):
        tx = get_entity(self).getGraphDatabase().beginTx()
        try:
            result = method(self, *args, **kwargs)
            tx.success()
            return result
        finally:
            tx.finish()
    return transactional


class Property(Object):
    def __new__(PropType,type=None,indexed=False,default=lambda:None,**params):
        if PropType is Property: PropType = property_type(type)
        if indexed is True:
            indexed = lambda name: name
        elif not indexed:
            indexed = None
        elif isinstance(indexed, str):
            indexed = (lambda indexed:(lambda name:indexed))(indexed)
        else:
            raise TypeError(
                "'indexed' should be either True, False or an index key.")
        if not hasattr(default,'__call__'):
            # we accept 'default' parameters that are values, but from hereon
            # this class expects 'default' to be a function that returns the
            # value when called with no parameters.
            default = (lambda x:(lambda:x))(default)
        return Factory(PropType, (indexed, default), params)
    def __init__(self, Entity, name, indexed, default):
        self.__name = name
        if indexed:
            self.__index = Entity._index
            self.__index_key = indexed(name)
        self.__default = default
    __index = None

    def __get__(self, obj, type=None):
        if obj is None: return self
        try:
            value = get_entity(obj)[self.__name]
        except:
            value = self.__default()
        return self.get(value)
    def __set__(self, obj, value):
        entity = get_entity(obj)
        value = self.set(value)
        entity[self.__name] = value
        if self.__index:
            graphdb = entity.getGraphDatabase()
            self.__index(graphdb).add(entity, self.__index_key, value)
    def __delete__(self, obj):
        entity = get_entity(obj)
        del entity[self.__name]
        if self.__index:
            graphdb = entity.getGraphDatabase()
            self.__index(graphdb).remove(entity, self.__index_key)

    def get(self, value):
        return value
    def set(self, value):
        return value

    def query(self, key, value, lookup, filters):
        if '__' in key:
            key, op = key.split('__',1)
        else:
            op = 'eq'
        if self.__index:
            if op == 'eq':
                op = ':'
            else:
                raise TypeError("Unsupported query operator '%s'" % op)
            if isinstance(value, strings):
                value = '"%s"' % value # TODO: we might need escaping
            lookup.append("%s%s%s" % (self.__index_key,op,value))
        else:
            if op == 'eq':
                predicate = lambda entity: self.get(entity[key]) == value
            else:
                raise TypeError("Unsupported query operator '%s'" % op)
            filters.append(predicate)

def property_type(type):
    if type is None: return DynamicProperty
    if issubclass(type, strings): return StringProperty
    if type in integers: return NumberProperty
    if type is float: return NumberProperty
    if type is datetime.datetime: return DateProperty

class DynamicProperty(Property):
    def set(self, value):
        if isinstance(value, strings): return value
        if isinstance(value, integers): return value
        if isinstance(value, float): return value
        raise ValueError("Invalid property type: '%s'" % type(value).__name__)

class StringProperty(Property):
    def get(self, value):
        return str(value)
    def set(self, value):
        if not isinstance(value, strings):
            raise ValueError("Invalid property type: '%s' is not a string"
                             % type(value).__name__)
        return value

class NumberProperty(Property):
    def get(self, value):
        if isinstance(value, float):
            return value
        else:
            return int(value)
    def set(self, value):
        if not (isinstance(value, integers) or isinstance(value, float)):
            raise ValueError("Invalid property type: '%s' is not a number"
                             % type(value).__name__)
        return value

class DateProperty(Property):
    def __init__(self, entity, name, indexed, default, tz=None):
        Property.__init__(self, entity, name, indexed, default)
        self.__tz = tz
    def get(self, value):
        return datetime.datetime.fromtimestamp(float(value))
    def set(self, value):
        if not isinstance(value, datetime.datetime):
            raise ValueError("Invalid property type: '%s' is not a datetime"
                             % type(value).__name__)
        return int(time.mktime(value.timetuple()))

