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

"""Neo4j traversal dsl for Python.
"""

from neo4j import _backend

# Bind the API to the underlying implementation
incoming = _backend.Direction(_backend.Direction.INCOMING)
outgoing = _backend.Direction(_backend.Direction.OUTGOING)
any      = _backend.Direction(_backend.Direction.ANY)

del _backend

from neo4j._backend import Evaluator, TraversalDescriptionImpl, implements, Evaluation, rel_type, Uniqueness

class PathEvaluator(implements(Evaluator)):

    def __init__(self, pattern):
        self._pattern = pattern
        self._pattern_length = len(self._pattern)
    
    def evaluate(self, path):
        segment = path.length()
        
        if segment == 0:
            return Evaluation.EXCLUDE_AND_CONTINUE
        
        if segment <= self._pattern_length:
          if self._pattern[segment-1].evaluate(path) == True:
              if segment == self._pattern_length:
                  return Evaluation.INCLUDE_AND_PRUNE
              else:
                  return Evaluation.EXCLUDE_AND_CONTINUE

        return Evaluation.EXCLUDE_AND_PRUNE


class PathPattern(object):
    ''' Main class in the traversal dsl. This acts
    as the base class for all filters, and is the
    class that implements the logic for joining
    filters together.
    '''
    def __init__(self, *pattern):
        self._pattern = pattern
        
    def __div__(self, other):
        if isinstance(other, PathPattern):
            other = other._pattern
        elif isinstance(other, (list,tuple)):
            other = tuple(other)
        else:
            other = other,
        return PathPattern(*(self._pattern + other))
    __truediv__ = __div__
    
    def __repr__(self):
        return ' / '.join([repr(item) for item in self._pattern])
        
    def description(self):
        desc = TraversalDescriptionImpl()
        desc = desc.uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
        return desc.evaluator(PathEvaluator(self._pattern))


class BoundPathTraversal(object):
    ''' Ties a path pattern to a single node, 
    and exposes methods to traverse the node
    using the pattern.
    '''
    def __init__(self, node, pattern):
        self.__node = node
        if not isinstance(pattern, PathPattern): 
            pattern = PathPattern(pattern)
        self._pattern = pattern
        
    def __div__(self, other):
        return BoundPathTraversal( self.__node, self._pattern.__div__(other) )
    __truediv__ = __div__
    
    def __repr__(self):
        return '%r / %r' % (self.__node, self._pattern)
        
    def __iter__(self):    
        return iter(self.traverser())
        
    @property
    def nodes(self):
        return self.traverser().nodes
            
    @property
    def relationships(self):
        return self.traverser().relationships
        
    def traverser(self):
        return self._pattern.description().traverse(self.__node)
        
        
# Filter utils

def matches_properties(e, properties):
    for key, value in properties.items():
        if not e.hasProperty(key) or e.getProperty(key) != value:
            return False
            
    return True
        
# Filters

class RelationshipFilter(PathPattern):
    def __init__(self, reltype, direction, **props):
        if not isinstance(reltype, (str, unicode)):
            reltype = reltype.name()
        self.type = reltype
        self.direction = direction
        self.props = props
        self._pattern = self,
     
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.type)
        
    def evaluate(self, path):
        rel = path.last_relationship
        if self.type == rel.getType().name() and \
           matches_properties(rel, self.props):
            return True
        return False
        
class Any(RelationshipFilter):
    def __init__(self, reltype=None, **kwargs):
        super(Any, self).__init__(reltype, any, **kwargs)
        
        
class Incoming(RelationshipFilter):
    def __init__(self, reltype=None, **kwargs):
        super(Incoming, self).__init__(reltype, incoming, **kwargs)
        

class Outgoing(RelationshipFilter):
    def __init__(self, reltype=None, **kwargs):
        super(Outgoing, self).__init__(reltype, outgoing, **kwargs)
        
