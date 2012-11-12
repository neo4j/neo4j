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
import tempfile, os

class GraphTest(unit_tests.GraphDatabaseTest):
    
    def test_create_db(self):
        folder_to_put_db_in = tempfile.mkdtemp()
        try:
            # START SNIPPET: creatingDatabase
            from neo4j import GraphDatabase
            
            # Create db
            db = GraphDatabase(folder_to_put_db_in)
            
            # Always shut down your database
            db.shutdown()
            # END SNIPPET: creatingDatabase
        finally:
           if os.path.exists(folder_to_put_db_in):
              import shutil
              shutil.rmtree(folder_to_put_db_in)
    
    def test_create_configured_db(self):
        folder_to_put_db_in = tempfile.mkdtemp()
        try:
            # START SNIPPET: creatingConfiguredDatabase
            from neo4j import GraphDatabase
            
            # Example configuration parameters
            db = GraphDatabase(folder_to_put_db_in, string_block_size=200, array_block_size=240)
            
            db.shutdown()
            # END SNIPPET: creatingConfiguredDatabase
        finally:
           if os.path.exists(folder_to_put_db_in):
              import shutil
              shutil.rmtree(folder_to_put_db_in)
        
    def test_with_statement_transactions(self):
        db = self.graphdb
        # START SNIPPET: withBasedTransactions
        # Start a transaction
        with db.transaction:
            # This is inside the transactional
            # context. All work done here
            # will either entirely succeed,
            # or no changes will be applied at all.
            
            # Create a node
            node = db.node()
            
            # Give it a name
            node['name'] = 'Cat Stevens'
            
        # The transaction is automatically
        # commited when you exit the with 
        # block.
        # END SNIPPET: withBasedTransactions
        self.assertNotEqual(node, None)
        
    def test_create_node(self):
        with self.graphdb.transaction:
            node = self.graphdb.node()
        self.assertNotEqual(node, None)
        
    def test_delete_node(self):
        db = self.graphdb
        # START SNIPPET: deleteNode
        with db.transaction:
            node = db.node()
            node.delete()
        # END SNIPPET: deleteNode
        
        try:
            self.graphdb.node[node.id]
            self.assertEqual(True,False)
        except Exception, e:
            self.assertTrue(isinstance(e, KeyError))
        
    def test_delete_node_by_id(self):
        db = self.graphdb
        with db.transaction:
            node = db.node()
        some_node_id = node.id
        
        # START SNIPPET: deleteByIdNode
        with db.transaction:
            del db.node[some_node_id]
        # END SNIPPET: deleteByIdNode
        
        try:
            self.graphdb.node[node.id]
            self.assertEqual(True,False)
        except Exception, e:
            self.assertTrue(isinstance(e, KeyError))
        
    def test_create_node_with_properties(self):
        db = self.graphdb
        # START SNIPPET: createNode
        with db.transaction:
            # Create a node
            thomas = db.node(name='Thomas Anderson', age=42)
        # END SNIPPET: createNode
        self.assertNotEqual(thomas, None)
        self.assertEquals(thomas['name'], 'Thomas Anderson')
        self.assertEquals(thomas['age'], 42)
        
    def test_properties(self):
        db = self.graphdb
        with db.transaction:
            node_or_rel = db.node()
        # START SNIPPET: setProperties
        with db.transaction:
            node_or_rel['name'] = 'Thomas Anderson'
            node_or_rel['age'] = 42
            node_or_rel['favourite_numbers'] = [1,2,3]
            node_or_rel['favourite_words'] = ['banana','blue']
        # END SNIPPET: setProperties
        
        # START SNIPPET: getProperties
        numbers = node_or_rel['favourite_numbers']
        # END SNIPPET: getProperties
        
        # START SNIPPET: deleteProperties
        with db.transaction:
            del node_or_rel['favourite_numbers']
        # END SNIPPET: deleteProperties
            del node_or_rel['favourite_words']
        
        # START SNIPPET: loopProperties
        # Loop key and value at the same time
        for key, value in node_or_rel.items():
            pass
            
        # Loop property keys
        for key in node_or_rel.keys():
            pass
            
        # Loop property values
        for value in node_or_rel.values():
            pass
        # END SNIPPET: loopProperties
        
        items = list(node_or_rel.items())
        self.assertEqual(len(items), 2)
        self.assertEqual(items[1][0],'age')
        self.assertEqual(items[1][1],42)
        
        keys = list(node_or_rel.keys())
        self.assertEqual(len(keys), 2)
        self.assertEqual(keys[1],'age')
            
        values = list(node_or_rel.values())
        self.assertEqual(len(values), 2)
        self.assertEqual(values[1],42)
        
    def test_remove_properties(self):
        with self.graphdb.transaction:
            node = self.graphdb.node(name='Thomas Anderson', age=42)
        
            self.assertEqual(node['name'], 'Thomas Anderson')
            del node['name']
            
            try:
                node['name']
                self.assertTrue(False)
            except Exception, e:
                self.assertTrue(isinstance(e, KeyError))
                
    def test_get_node_by_id(self):
        db = self.graphdb
        with db.transaction:
            node = db.node()
        some_node_id = node.id
        # START SNIPPET: getNodeById
        # You don't have to be in a transaction
        # to do read operations.
        a_node = db.node[some_node_id]
        # END SNIPPET: getNodeById
        self.assertNotEqual(a_node, None)
        
    def test_get_reference_node(self):
        db = self.graphdb
        # START SNIPPET: getReferenceNode
        reference = db.reference_node
        # END SNIPPET: getReferenceNode
        self.assertNotEqual(reference, None)
        
    def test_can_create_relationship(self):
        db = self.graphdb
        
        # START SNIPPET: createRelationship
        with self.graphdb.transaction:
            # Nodes to create a relationship between
            steven = self.graphdb.node(name='Steve Brook')
            poplar_bluff = self.graphdb.node(name='Poplar Bluff')
            
            # Create a relationship of type "mayor_of"
            relationship = steven.mayor_of(poplar_bluff, since="12th of July 2012")
            
            # Or, to create relationship types with names
            # that would not be possible with the above
            # method.
            steven.relationships.create('mayor_of', poplar_bluff, since="12th of July 2012")
        # END SNIPPET: createRelationship
            secondrel = poplar_bluff.likes(steven, message="buh")
        
        message = ''
        for rel in steven.mayor_of:
            message += "%s %s %s" % (
                rel.start['name'],
                rel['since'],
                rel.end['name'],
                )
        self.assertEquals(message, "Steve Brook 12th of July 2012 Poplar BluffSteve Brook 12th of July 2012 Poplar Bluff")
        
        a_node = steven
        # START SNIPPET: accessingRelationships
        # All relationships on a node
        for rel in a_node.relationships:
            pass
            
        # Incoming relationships
        for rel in a_node.relationships.incoming:
            pass
            
        # Outgoing relationships
        for rel in a_node.relationships.outgoing:
            pass
            
        # Relationships of a specific type
        for rel in a_node.mayor_of:
            pass
            
        # Incoming relationships of a specific type
        for rel in a_node.mayor_of.incoming:
            pass
            
        # Outgoing relationships of a specific type
        for rel in a_node.mayor_of.outgoing:
            pass
        # END SNIPPET: accessingRelationships
        
        self.assertEquals(len(steven.relationships), 3)
        self.assertEquals(len(steven.relationships.incoming), 1)
        self.assertEquals(len(steven.relationships.outgoing), 2)
        
        self.assertEquals(len(steven.likes), 1)
        self.assertEquals(len(steven.likes.incoming), 1)
        self.assertEquals(len(steven.likes.outgoing), 0)
        
    def test_relationship_attributes(self):
        db = self.graphdb
        
        with self.graphdb.transaction:
            source = self.graphdb.node()
            target = self.graphdb.node()
            
            # Create a relationship of type "related_to"
            relationship = source.related_to(target)
        # START SNIPPET: relationshipAttributes
        relationship_type = relationship.type
        
        start_node = relationship.start
        end_node = relationship.end
        # END SNIPPET: relationshipAttributes
        
        rel = relationship
        self.assertEquals(rel.type.name(), 'related_to')
        self.assertEquals(rel.start, source)
        self.assertEquals(rel.end, target)
        
    def test_get_relationship_by_id(self):
        db = self.graphdb
        with self.graphdb.transaction:
            source = self.graphdb.node()
            target = self.graphdb.node()
            rel = source.Knows(target)
        a_relationship_id = rel.id
        # START SNIPPET: getRelationshipById
        the_relationship = db.relationship[a_relationship_id]
        # END SNIPPET: getRelationshipById
        self.assertNotEqual(the_relationship, None)
        
    def test_delete_relationship(self):
        db = self.graphdb
        # START SNIPPET: deleteRelationship
        with db.transaction:
            # Create a relationship
            source = db.node()
            target = db.node()
            rel = source.Knows(target)
            
            # Delete it
            rel.delete()
        # END SNIPPET: deleteRelationship
            
        try:
            self.graphdb.relationship[rel.id]
            self.assertTrue(False)
        except Exception, e:
            self.assertTrue(isinstance(e, KeyError))
        
    def test_delete_relationship_by_id(self):
        
        db = self.graphdb
        
        with self.graphdb.transaction:
            node1 = self.graphdb.node()
            node2 = self.graphdb.node()
            rel = node1.Knows(node2)
            
        some_relationship_id = rel.id
        # START SNIPPET: deleteByIdRelationship
        with db.transaction:
            del db.relationship[some_relationship_id]
        # END SNIPPET: deleteByIdRelationship
            
        try:
            self.graphdb.relationship[rel.id]
            self.assertTrue(False)
        except Exception, e:
            self.assertTrue(isinstance(e, KeyError))

if __name__ == '__main__':
    unit_tests.unittest.main()
