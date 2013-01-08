# -*- mode: Python; coding: utf-8 -*-

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

from __future__ import with_statement

import unit_tests
import neo4j
from neo4j import Direction, Evaluation, Uniqueness

class IndexTest(unit_tests.GraphDatabaseTest):

    def test_index_exists(self):
        db = self.graphdb
        
        with db.transaction:
            db.relationship.indexes.create('my_rels')
            db.node.indexes.create('my_nodes')
        
        with db.transaction:
            # Create a relationship index
            self.assertTrue(db.relationship.indexes.exists('my_rels'))
            self.assertFalse(db.relationship.indexes.exists('asd'))
            
            # START SNIPPET: checkIfIndexExists
            exists = db.node.indexes.exists('my_nodes')
            # END SNIPPET: checkIfIndexExists
            self.assertTrue(exists)
            self.assertFalse(db.node.indexes.exists('asd'))

    def test_create_fulltext_index(self):
        db = self.graphdb
        
        # START SNIPPET: createIndex
        with db.transaction:
            # Create a relationship index
            rel_idx = db.relationship.indexes.create('my_rels')
            
            # Create a node index, passing optional
            # arguments to the index provider.
            # In this case, enable full-text indexing.
            node_idx = db.node.indexes.create('my_nodes', type='fulltext')
        # END SNIPPET: createIndex
        # START SNIPPET: getIndex
        with db.transaction:
            node_idx = db.node.indexes.get('my_nodes') 
            
            rel_idx = db.relationship.indexes.get('my_rels')
        # END SNIPPET: getIndex
        # START SNIPPET: deleteIndex
        with db.transaction:
            node_idx = db.node.indexes.get('my_nodes') 
            node_idx.delete()
            
            rel_idx = db.relationship.indexes.get('my_rels')
            rel_idx.delete()
        # END SNIPPET: deleteIndex
            idx = node_idx = db.node.indexes.create('test', type='fulltext')
            idx['akey']['A name of some kind.'] = self.graphdb.node()
            
        it = idx.query("akey:name")
        
        self.assertTrue(len(it), 1)
        it.close()

    def test_index_node(self):
        db = self.graphdb
        # START SNIPPET: addToIndex
        with db.transaction:
            # Indexing nodes
            a_node = db.node()
            node_idx = db.node.indexes.create('my_nodes')
            
            # Add the node to the index
            node_idx['akey']['avalue'] = a_node
            
            # Indexing relationships
            a_relationship = a_node.knows(db.node())
            rel_idx = db.relationship.indexes.create('my_rels')
            
            # Add the relationship to the index
            rel_idx['akey']['avalue'] = a_relationship
        # END SNIPPET: addToIndex
        
        ret = list(node_idx['akey']['avalue'])[0]
        self.assertEqual(ret.id, a_node.id)
        

    def test_remove_node_index(self):
        with self.graphdb.transaction:
            n = self.graphdb.node()
            
            idx1 = self.graphdb.node.indexes.create('test1')
            idx1['akey']['avalue'] = n
            
            idx1.delete()
            
        try:
            self.graphdb.node.indexes.get('test1')
            self.assertTrue(False)
        except Exception, e:
            self.assertTrue(isinstance(e, ValueError))
            
    def test_query_for_node(self):
        with self.graphdb.transaction:
            n = self.graphdb.node()
            
            idx = self.graphdb.node.indexes.create('test')
            idx['akey']['avalue'] = n
            
        ret = list(idx.query('akey:avalue'))[0]
        self.assertEqual(ret.id, n.id)
            
    def test_get_first_index_hit(self):
        with self.graphdb.transaction:
        
            idx = self.graphdb.node.indexes.create('test')
            for x in range(50):
                idx['akey']['avalue'] = self.graphdb.node(name=x)
        
        hits = iter(idx['akey']['avalue'])
        node = hits.next()
        hits.close()
        
        self.assertEquals(0,node['name'])
            
    def test_iterate_index(self):
        with self.graphdb.transaction:

            idx = self.graphdb.node.indexes.create('test')
            for x in range(50):
                idx['akey']['avalue'] = self.graphdb.node(name=x)

        hits = []
        for hit in idx['akey']['avalue']:
            hits.append(hit)

        self.assertEquals(50,len(hits))
            
    def test_slice_query_result(self):
        with self.graphdb.transaction:
        
            idx = self.graphdb.node.indexes.create('test')
            for x in range(50):
                idx['akey']['avalue'] = self.graphdb.node(name=x)
            
        
        # START SNIPPET: query
        hits = idx.query('akey:avalue')
        for item in hits:
            pass
        
        # Always close index results when you are 
        # done, to free up resources.
        hits.close()
        # END SNIPPET: query
        
        hits = idx.query('akey:avalue')
        self.assertEqual(len(list(hits[:10])), 10)
        hits.close()
            
    def test_slice_get_result(self):
        with self.graphdb.transaction:
        
            idx = self.graphdb.node.indexes.create('test')
            for x in range(50):
                idx['akey']['avalue'] = self.graphdb.node()
            
        it = idx['akey']['avalue']
        
        self.assertTrue(len(list(it[:10])), 10)
        it.close()
        
        # START SNIPPET: directLookup
        hits = idx['akey']['avalue']
        for item in hits:
            pass
        
        # Always close index results when you are 
        # done, to free up resources.
        hits.close()
        # END SNIPPET: directLookup
            
    def test_get_first_result(self):
        with self.graphdb.transaction:
        
            idx = self.graphdb.node.indexes.create('test')
            for x in range(50):
                idx['akey']['avalue'] = self.graphdb.node()
            
        it = idx['akey']['avalue']
        
        self.assertTrue(it[0] is not None)
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
            
            # START SNIPPET: removeFromIndex
            # Remove specific key/value/item triplet
            del idx['akey']['avalue'][item]
            
            # Remove all instances under a certain
            # key
            del idx['akey'][item]
            
            # Remove all instances all together
            del idx[item]
            # END SNIPPET: removeFromIndex
        
if __name__ == '__main__':
    unit_tests.unittest.main()
