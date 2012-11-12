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

from __future__ import with_statement

import unit_tests
import neo4j
from neo4j import Direction, Evaluation, Uniqueness

class TraversalTest(unit_tests.GraphDatabaseTest):

    def create_data(self):
        with self.graphdb.transaction:
            self.source = self.graphdb.node(message='hello')
            target = self.graphdb.node(message='world')
            relationship = self.source.related_to(target, message="graphy")
            secondrel = target.likes(self.source, message="buh")

    def test_traverse_string_types(self):
        self.create_data()
        
        db = self.graphdb
        start_node = self.source
        
        # START SNIPPET: basicTraversal
        traverser = db.traversal()\
            .relationships('related_to')\
            .traverse(start_node)
            
        # The graph is traversed as 
        # you loop through the result.
        for node in traverser.nodes:
            pass
        # END SNIPPET: basicTraversal
            
        self.assertEqual(len(list(traverser.nodes)), 2)
        
        # START SNIPPET: directedTraversal
        from neo4j import OUTGOING, INCOMING, ANY
        
        traverser = db.traversal()\
            .relationships('related_to', OUTGOING)\
            .traverse(start_node)
        # END SNIPPET: directedTraversal
        
        self.assertEqual(len(list(traverser.nodes)), 2)
        
        # START SNIPPET: multiRelationshipTraversal
        from neo4j import OUTGOING, INCOMING, ANY
        
        traverser = db.traversal()\
            .relationships('related_to', INCOMING)\
            .relationships('likes')\
            .traverse(start_node)
        # END SNIPPET: multiRelationshipTraversal
        self.assertEqual(len(list(traverser.nodes)), 2)
        
        # START SNIPPET: traversalResults
        traverser = db.traversal()\
            .relationships('related_to')\
            .traverse(start_node)
            
        # Get each possible path
        for path in traverser:
            pass
            
        # Get each node
        for node in traverser.nodes:
            pass
            
        # Get each relationship
        for relationship in traverser.relationships:
            pass
        # END SNIPPET: traversalResults

    def test_traverse_programmatic_types(self):
        self.create_data()
        
        t = self.graphdb.traversal()\
            .depthFirst()\
            .relationships(Direction.ANY.related_to)\
            .traverse(self.source)
            
        res = list(t.nodes)
        self.assertEqual(len(res), 2)
        
    def test_dynamic_evaluator(self):
        self.create_data()
        db = self.graphdb
        start_node = self.source
        
        # START SNIPPET: evaluators
        from neo4j import Evaluation
        
        # Evaluation contains the four
        # options that an evaluator can
        # return. They are:
        
        Evaluation.INCLUDE_AND_CONTINUE
        # Include this node in the result and 
        # continue the traversal
        
        Evaluation.INCLUDE_AND_PRUNE
        # Include this node in the result, but don't 
        # continue the traversal
        
        Evaluation.EXCLUDE_AND_CONTINUE
        # Exclude this node from the result, but 
        # continue the traversal
        
        Evaluation.EXCLUDE_AND_PRUNE
        # Exclude this node from the result and 
        # don't continue the traversal 
        
        # An evaluator
        def my_evaluator(path):
            # Filter on end node property
            if path.end['message'] == 'world':
                return Evaluation.INCLUDE_AND_CONTINUE
            
            # Filter on last relationship type
            if path.last_relationship.type.name() == 'related_to': 
                return Evaluation.INCLUDE_AND_PRUNE
            
            # You can do even more complex things here, like subtraversals.
            
            return Evaluation.EXCLUDE_AND_CONTINUE
            
        # Use the evaluator
        traverser = db.traversal()\
            .evaluator(my_evaluator)\
            .traverse(start_node)
        # END SNIPPET: evaluators
        
        def exclude_all(path):
            return Evaluation.EXCLUDE_AND_CONTINUE
        
        def include_all(path):
            return Evaluation.INCLUDE_AND_CONTINUE
        
        t = self.graphdb.traversal()\
            .depthFirst()\
            .evaluator(include_all)\
            .traverse(self.source)

        res = list(t.nodes)
        self.assertEqual(len(res), 2)
        
        t = self.graphdb.traversal()\
            .depthFirst()\
            .evaluator(exclude_all)\
            .traverse(self.source)
            
        res = list(t.nodes)
        self.assertEqual(len(res), 0)
        
    def test_uniqueness(self):
        self.create_data()
        db = self.graphdb
        start_node = self.source
        # START SNIPPET: uniqueness
        from neo4j import Uniqueness
        
        # Available options are:
        
        Uniqueness.NONE 
        # Any position in the graph may be revisited.
        
        Uniqueness.NODE_GLOBAL 
        # Default option
        # No node in the entire graph may be visited 
        # more than once. This could potentially 
        # consume a lot of memory since it requires 
        # keeping an in-memory data structure 
        # remembering all the visited nodes.
        
        Uniqueness.RELATIONSHIP_GLOBAL 
        # No relationship in the entire graph may be 
        # visited more than once. For the same 
        # reasons as NODE_GLOBAL uniqueness, this 
        # could use up a lot of memory. But since 
        # graphs typically have a larger number of 
        # relationships than nodes, the memory 
        # overhead of this uniqueness level could 
        # grow even quicker.
        
        Uniqueness.NODE_PATH 
        # A node may not occur previously in the 
        # path reaching up to it.
        
        Uniqueness.RELATIONSHIP_PATH 
        # A relationship may not occur previously in 
        # the path reaching up to it.
        
        Uniqueness.NODE_RECENT 
        # Similar to NODE_GLOBAL uniqueness in that 
        # there is a global collection of visited 
        # nodes each position is checked against. 
        # This uniqueness level does however have a 
        # cap on how much memory it may consume in 
        # the form of a collection that only 
        # contains the most recently visited nodes. 
        # The size of this collection can be 
        # specified by providing a number as the 
        # second argument to the 
        # uniqueness()-method along with the 
        # uniqueness level.
        
        Uniqueness.RELATIONSHIP_RECENT 
        # works like NODE_RECENT uniqueness, but 
        # with relationships instead of nodes. 
        
        
        traverser = db.traversal()\
            .uniqueness(Uniqueness.NODE_PATH)\
            .traverse(start_node)
        # END SNIPPET: uniqueness
            
        res = list(traverser.nodes)
        self.assertEqual(len(res), 3)
        
        
    def test_ordering(self):
        self.create_data()
        db = self.graphdb
        start_node = self.source
        
        # START SNIPPET: ordering
        # Depth first traversal, this
        # is the default.
        traverser = db.traversal()\
            .depthFirst()\
            .traverse(self.source)
            
        # Breadth first traversal
        traverser = db.traversal()\
            .breadthFirst()\
            .traverse(start_node)
        # END SNIPPET: ordering
            
        res = list(traverser.nodes)
        self.assertEqual(len(res), 2)
        
    def test_paths(self):
        self.create_data()
        
        t = self.graphdb.traversal()\
            .traverse(self.source)
            
        for path in t:
            # START SNIPPET: accessPathStartAndEndNode
            start_node = path.start
            end_node = path.end
            # END SNIPPET: accessPathStartAndEndNode
            self.assertNotEqual(start_node, None)
            self.assertNotEqual(end_node, None)
            # START SNIPPET: accessPathLastRelationship
            last_relationship = path.last_relationship
            # END SNIPPET: accessPathLastRelationship
            
            # START SNIPPET: loopThroughPath
            for item in path:
                # Item is either a Relationship,
                # or a Node
                pass
                
            for nodes in path.nodes:
                # All nodes in a path
                pass
                
            for nodes in path.relationships:
                # All relationships in a path
                pass
            # END SNIPPET: loopThroughPath
            
            break

    def test_import_decision_shortcut(self):
        from neo4j.traversal import INCLUDE_AND_CONTINUE, INCLUDE_AND_PRUNE, EXCLUDE_AND_CONTINUE, EXCLUDE_AND_PRUNE
        self.create_data()
        db = self.graphdb

        def iac(path):
            return INCLUDE_AND_CONTINUE

        def iap(path):
            return INCLUDE_AND_PRUNE

        def eac(path):
            return EXCLUDE_AND_CONTINUE            

        def eap(path):
            return EXCLUDE_AND_PRUNE

        traverser = db.traversal()\
            .evaluator(iac)\
            .evaluator(iap)\
            .evaluator(eac)\
            .evaluator(eap)\
            .traverse(self.source)
            
        res = list(traverser.nodes)
        self.assertEqual(len(res), 0)
        
        
if __name__ == '__main__':
    unit_tests.unittest.main()
