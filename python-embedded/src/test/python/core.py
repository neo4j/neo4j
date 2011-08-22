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

class GraphTest(unit_tests.GraphDatabaseTest):
    def test_create_node(self):
        with self.graphdb.transaction:
            node = self.graphdb.node()
        self.assertNotEqual(node, None)
        
    def test_create_node_with_properties(self):
        with self.graphdb.transaction:
            node = self.graphdb.node(name='Thomas Anderson', age=42)
        self.assertNotEqual(node, None)
        self.assertEquals(node['name'], 'Thomas Anderson')
        self.assertEquals(node['age'], 42)
        
    def test_iterate_properties(self):
        with self.graphdb.transaction:
            node = self.graphdb.node(name='Thomas Anderson', age=42)
        items = list(node.items())
        self.assertEqual(len(items), 2)
        self.assertEqual(items[1][0],'name')
        self.assertEqual(items[1][1],'Thomas Anderson')
        
        keys = list(node.keys())
        self.assertEqual(len(keys), 2)
        self.assertEqual(keys[1],'name')
            
        values = list(node.values())
        self.assertEqual(len(values), 2)
        self.assertEqual(values[1],'Thomas Anderson')
        
    def test_get_node_by_id(self):
        with self.graphdb.transaction:
            node = self.graphdb.node()
        self.graphdb.node[node.id]
        
    def test_can_create_relationship(self):
        with self.graphdb.transaction:
            source = self.graphdb.node(message='hello')
            target = self.graphdb.node(message='world')
            relationship = source.related_to(target, message="graphy")
            secondrel = target.likes(source, message="buh")
        message = ''
        for rel in source.related_to:
            message += "%s %s %s" % (
                rel.start['message'],
                rel['message'],
                rel.end['message'],
                )
        self.assertEquals(message, "hello graphy world")
        self.assertEquals(rel.type.name(), 'related_to')
        
        self.assertEquals(len(list(source.rels)), 2)
        self.assertEquals(len(list(source.rels.incoming)), 1)
        self.assertEquals(len(list(source.rels.outgoing)), 1)
        
        self.assertEquals(len(list(source.likes)), 1)
        self.assertEquals(len(list(source.likes.incoming)), 1)
        self.assertEquals(len(list(source.likes.outgoing)), 0)

if __name__ == '__main__':
    unit_tests.unittest.main()
