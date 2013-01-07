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

from _backend import extends, implements, rel_type, TraverserImpl,\
                     Traversal, TraversalDescriptionImpl, strings,\
                     Evaluation, Evaluator, Uniqueness,\
                     TraversalBranchImpl, BidirectionalTraversalBranchPath,\
                     ExtendedPath, SingleNodePath, FinalTraversalBranch,\
                     AsOneStartBranch, StartNodeTraversalBranch, WrappedPath
        
from util import PythonicIterator
        
from core import Direction, DirectionalType

# Give the user references to the 
# direct evaluator decision choices.
INCLUDE_AND_CONTINUE = Evaluation.INCLUDE_AND_CONTINUE
INCLUDE_AND_PRUNE = Evaluation.INCLUDE_AND_PRUNE
EXCLUDE_AND_CONTINUE = Evaluation.EXCLUDE_AND_CONTINUE
EXCLUDE_AND_PRUNE = Evaluation.EXCLUDE_AND_PRUNE

class DynamicEvaluator(implements(Evaluator)):
    def __init__(self, eval_method):
        self._eval_method = eval_method

    def evaluate(self, path):
        return self._eval_method(path)

#
# Pythonification of the traversal API
#

# This is a messy hack, but will only have to be here until
# 1.9, when the traversal support in python is dropped. After that,
# though, the WrappedPath class still needs something like this for
# cypher support.

for PathClass in [TraversalBranchImpl,BidirectionalTraversalBranchPath,\
                  ExtendedPath,SingleNodePath,FinalTraversalBranch,\
                  AsOneStartBranch,StartNodeTraversalBranch,\
                  WrappedPath]:

    class IrrelevantClassName(extends(PathClass)):

        @property
        def start(self): return self.startNode()
        
        @property
        def end(self):   return self.endNode()
        
        @property
        def last_relationship(self): return self.lastRelationship()
        
        @property
        def nodes(self): 
            it = self._super__nodes().iterator()
            while it.hasNext():
                yield it.next()
        
        @property
        def relationships(self):
            it = self._super__relationships().iterator()
            while it.hasNext():
                yield it.next()
        
        def __str__(self):
            out = []
            current = self.start
            for rel in self.relationships:
                out.append( '({0})'.format(current.id) )
                if rel.start == current:
                    out.append('-[{0},{1}]->'.format(rel.type, rel.id))
                else:
                    out.append('<-[{0},{1}]-'.format(rel.type, rel.id))
                current = rel.other_node(current)
            
            # Print last node
            out.append( '({0})'.format(current.id) )
            
            return ''.join(out)
        
        def __repr__(self): return self.toString()
        
        def __len__(self):  return self.length()
        
        def __iter__(self):
            it = self.iterator()
            while it.hasNext():
                yield it.next() 

# namespace cleanup
del IrrelevantClassName
# /namespace cleanup

      
class TraversalDescriptionImpl(extends(TraversalDescriptionImpl)):
    
    def relationships(self, reltype, direction=Direction.ANY):
    
        if type(reltype) in strings:
            reltype = rel_type(reltype)
        elif isinstance(reltype, DirectionalType):
            direction = reltype.dir
            reltype = reltype.type
        return self._super__relationships(reltype, direction)
        
    def evaluator(self, ev):
        if hasattr(ev, '__call__'):
            ev = DynamicEvaluator(ev)
        return self._super__evaluator(ev)

    def traverse(self, *start_nodes):
        return self._super__traverse(start_nodes)
        
class TraverserImpl(extends(TraverserImpl)):
    
    def __iter__(self):
        it = self.iterator()
        while it.hasNext():
            yield it.next()
            
    @property
    def last_relationship(self): return self.lastRelationship()
    
    @property
    def nodes(self): return self._super__nodes()
    
    @property
    def relationships(self): return self._super__relationships()
