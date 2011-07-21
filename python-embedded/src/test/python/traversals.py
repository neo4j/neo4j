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

__all__ = 'SomeTests',

import unit_tests, datetime

from neo4j import *

class SomeTests(unit_tests.GraphDatabaseTest):
    def test_traverse_with_rel_filter(self):
        source = self.create_data()
        
        traverser = source / Any('related_to', message='graphy')
        self.assertEqual(1, len(list(traverser)))
        
        for path in traverser:
            relationship = path.last_relationship
            self.assertEquals(relationship['message'], 'graphy')
            self.assertEquals(path.start['message'], 'hello')
            self.assertEquals(path.end['message'], 'world')
            
            nodes = list(path.nodes())
            self.assertEqual(2, len(nodes))
            self.assertEquals(nodes[0]['message'], 'hello')
            self.assertEquals(nodes[1]['message'], 'world')
            
            rels = list(path.relationships())
            self.assertEqual(1, len(rels))
            self.assertEquals(rels[0]['message'], 'graphy')
          
        for node in traverser.nodes():
            self.assertEquals(node['message'], 'world')
            
        for rel in traverser.relationships():
            self.assertEquals(rel['message'], 'graphy')
            
    def test_traverse_with_node_filter(self):
        source = self.create_data()
        
        traverser = source / Any('related_to')(message='world')
        
        self.assertEqual(1, len(list(traverser)))
        
        for path in traverser:
            relationship = path.last_relationship
            self.assertEquals(relationship['message'], 'blah')
            self.assertEquals(path.start['message'], 'hello')
            self.assertEquals(path.end['message'], 'world')
            self.assertEqual(2, len(list(path.nodes())))
            
    def create_data(self):
        with self.graphdb.transaction:
            source = self.graphdb.node(message='hello')
            target = self.graphdb.node(message='world')
            toomuch = self.graphdb.node(message='blaj')
            source.related_to(target, message="blah")
            source.related_to(target, message="graphy")
            source.related_to(target)
            
            target.related_to(toomuch)
        return source

if __name__ == '__main__':
    unit_tests.unittest.main()
