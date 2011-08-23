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
from neo4j import ANY, OUTGOING, INCOMING

class GraphTest(unit_tests.GraphDatabaseTest):

    def create_data(self):
        with self.graphdb.transaction:
            self.source = self.graphdb.node(message='hello')
            target = self.graphdb.node(message='world')
            relationship = self.source.related_to(target, message="graphy")
            secondrel = target.likes(self.source, message="buh")

    def test_traverse_string_types(self):
        self.create_data()
        
        t = neo4j.Traversal.description()\
            .depthFirst()
        
        res = list(t\
            .relationships('related_to')\
            .traverse(self.source)\
            .nodes())
        self.assertEqual(len(res), 2)
        
        res = list(t\
            .relationships('related_to', OUTGOING)\
            .traverse(self.source)\
            .nodes())
        self.assertEqual(len(res), 2)
        
        res = list(t\
            .relationships('related_to', INCOMING)\
            .traverse(self.source)\
            .nodes())
        self.assertEqual(len(res), 1)
        

    def test_traverse_programmatic_types(self):
        self.create_data()
        
        t = neo4j.Traversal.description()\
            .depthFirst()\
            .relationships(ANY.related_to)\
            .traverse(self.source)
            
        res = list(t.nodes())
        self.assertEqual(len(res), 2)
        
        
        
if __name__ == '__main__':
    unit_tests.unittest.main()
