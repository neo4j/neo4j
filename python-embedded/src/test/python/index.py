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

class IndexTest(unit_tests.GraphDatabaseTest):

    def test_index_node(self):
        with self.graphdb.transaction:
            n = self.graphdb.node()
            
            idx = self.graphdb.node.indexes.create('test')
            idx['akey']['avalue'] = n
            
        ret = list(idx['akey']['avalue'])[0]
        self.assertEqual(ret.id, n.id)

    def test_remove_node_index(self):
        with self.graphdb.transaction:
            n = self.graphdb.node()
            
            idx1 = self.graphdb.node.indexes.create('test1')
            idx1['akey']['avalue'] = n
            
            idx1.delete()
            
        try:
            self.graphdb.node.indexes.get('test1')
            self.assertTrue(False)
        except Exception as e:
            self.assertTrue(isinstance(e, ValueError))
            
    def test_query_for_node(self):
        with self.graphdb.transaction:
            n = self.graphdb.node()
            
            idx = self.graphdb.node.indexes.create('test')
            idx['akey']['avalue'] = n
            
        ret = list(idx.query('akey:avalue'))[0]
        self.assertEqual(ret.id, n.id)
            
    def test_slice_query_result(self):
        with self.graphdb.transaction:
        
            idx = self.graphdb.node.indexes.create('test')
            for x in range(50):
                idx['akey']['avalue'] = self.graphdb.node()
            
        it = idx.query('akey:avalue')
        
        self.assertTrue(len(list(it[:10])), 10)
        it.close()
            
    def test_slice_get_result(self):
        with self.graphdb.transaction:
        
            idx = self.graphdb.node.indexes.create('test')
            for x in range(50):
                idx['akey']['avalue'] = self.graphdb.node()
            
        it = idx['akey']['avalue']
        
        self.assertTrue(len(list(it[:10])), 10)
        it.close()
            
    def test_remove_node_from_index(self):
        with self.graphdb.transaction:
            n = self.graphdb.node()
            
            idx = self.graphdb.node.indexes.create('test')
        self._index_and_remove_item(idx, n)
            
    def test_remove_relationship_from_index(self):
        with self.graphdb.transaction:
            n = self.graphdb.node()
            r = n.Knows(self.graphdb.node())
            idx = self.graphdb.relationship.indexes.create('test')
        self._index_and_remove_item(idx, r)
            
    def _index_and_remove_item(self, idx, item):
        with self.graphdb.transaction:
            idx['akey']['avalue'] = item
            idx['akey']['bvalue'] = item
            idx['bkey']['avalue'] = item
            
            self.assertEqual(len(list(idx['akey']['avalue'])), 1)
            self.assertEqual(len(list(idx['akey']['bvalue'])), 1)
            self.assertEqual(len(list(idx['bkey']['avalue'])), 1)
            
            del idx['akey']['avalue'][item]
            
            self.assertEqual(len(list(idx['akey']['avalue'])), 0)
            self.assertEqual(len(list(idx['akey']['bvalue'])), 1)
            self.assertEqual(len(list(idx['bkey']['avalue'])), 1)
            
            del idx['akey'][item]
            
            self.assertEqual(len(list(idx['akey']['avalue'])), 0)
            self.assertEqual(len(list(idx['akey']['bvalue'])), 0)
            self.assertEqual(len(list(idx['bkey']['avalue'])), 1)
            
            del idx[item]
            
            self.assertEqual(len(list(idx['akey']['avalue'])), 0)
            self.assertEqual(len(list(idx['akey']['bvalue'])), 0)
            self.assertEqual(len(list(idx['bkey']['avalue'])), 0)
        
if __name__ == '__main__':
    unit_tests.unittest.main()
