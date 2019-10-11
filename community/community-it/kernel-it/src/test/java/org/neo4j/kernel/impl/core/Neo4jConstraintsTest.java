/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.MyRelTypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Neo4jConstraintsTest extends AbstractNeo4jTestCase
{
    private final String key = "testproperty";

    @Test
    void testDeleteReferenceNodeOrLastNodeIsOk()
    {
        for ( int i = 0; i < 10; i++ )
        {
            createNode();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( Node node : transaction.getAllNodes() )
            {
                for ( Relationship rel : node.getRelationships() )
                {
                    rel.delete();
                }
                node.delete();
            }
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertFalse( transaction.getAllNodes().iterator().hasNext() );
            transaction.commit();
        }
    }

    @Test
    void testDeleteNodeWithRel1()
    {
        Node node1 = createNode();
        Node node2 = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode1 = transaction.getNodeById( node1.getId() );
            var txNode2 = transaction.getNodeById( node2.getId() );

            txNode1.createRelationshipTo( txNode2, MyRelTypes.TEST );
            txNode1.delete();
            assertThrows( Exception.class, transaction::commit );
        }
    }

    @Test
    void testDeleteNodeWithRel2()
    {
        Node node1 = createNode();
        Node node2 = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode1 = transaction.getNodeById( node1.getId() );
            var txNode2 = transaction.getNodeById( node2.getId() );

            txNode1.createRelationshipTo( txNode2, MyRelTypes.TEST );
            txNode2.delete();
            txNode1.delete();
            assertThrows( Exception.class, transaction::commit );
        }
    }

    @Test
    void testDeleteNodeWithRel3()
    {
        // make sure we can delete in wrong order
        Node node0 = createNode();
        Node node1 = createNode();
        Node node2 = createNode();
        Relationship rel1;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode0 = transaction.getNodeById( node0.getId() );
            var txNode1 = transaction.getNodeById( node1.getId() );
            var txNode2 = transaction.getNodeById( node2.getId() );

            Relationship rel0 = txNode0.createRelationshipTo( txNode1, MyRelTypes.TEST );
            rel1 = txNode0.createRelationshipTo( txNode2, MyRelTypes.TEST );
            txNode1.delete();
            rel0.delete();
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode0 = transaction.getNodeById( node0.getId() );
            var txNode2 = transaction.getNodeById( node2.getId() );
            var txRel1 = transaction.getRelationshipById( rel1.getId() );

            txNode2.delete();
            txRel1.delete();
            txNode0.delete();
            transaction.commit();
        }
    }

    @Test
    void testCreateRelOnDeletedNode()
    {
        Node node1 = createNode();
        Node node2 = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode = transaction.getNodeById( node1.getId() );
            txNode.delete();
            assertThrows( Exception.class, () -> txNode.createRelationshipTo( node2, MyRelTypes.TEST ) );
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            transaction.getNodeById( node2.getId() ).delete();
            transaction.getNodeById( node1.getId() ).delete();
            transaction.commit();
        }
    }

    @Test
    void testAddPropertyDeletedNode()
    {
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode = transaction.getNodeById( node.getId() );

            txNode.delete();
            assertThrows( Exception.class, () -> txNode.setProperty( key, 1 ) );
        }
    }

    @Test
    void testRemovePropertyDeletedNode()
    {
        GraphDatabaseService database = getGraphDb();
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode();
            node.setProperty( key, 1 );
            node.delete();
            assertThrows( Exception.class, () ->
            {
                node.removeProperty( key );
                transaction.commit();
            } );
        }
    }

    @Test
    void testChangePropertyDeletedNode()
    {
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode = transaction.getNodeById( node.getId() );
            txNode.setProperty( key, 1 );
            txNode.delete();
            assertThrows( Exception.class, () ->
            {
                txNode.setProperty( key, 2 );
                transaction.commit();
            } );
        }
    }

    @Test
    void testAddPropertyDeletedRelationship()
    {
        Node node1 = createNode();
        Node node2 = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode1 = transaction.getNodeById( node1.getId() );
            var txNode2 = transaction.getNodeById( node2.getId() );

            Relationship rel = txNode1.createRelationshipTo( txNode2, MyRelTypes.TEST );
            rel.delete();
            assertThrows( Exception.class, () ->
            {
                rel.setProperty( key, 1 );
                transaction.commit();
            } );
            txNode1.delete();
            txNode2.delete();
            transaction.commit();
        }
    }

    @Test
    void testRemovePropertyDeletedRelationship()
    {
        Node node1 = createNode();
        Node node2 = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode1 = transaction.getNodeById( node1.getId() );
            var txNode2 = transaction.getNodeById( node2.getId() );

            Relationship rel = txNode1.createRelationshipTo( txNode2, MyRelTypes.TEST );
            rel.setProperty( key, 1 );
            rel.delete();
            assertThrows( Exception.class, () ->
            {
                rel.removeProperty( key );
                transaction.commit();
            } );
            txNode1.delete();
            txNode2.delete();
            transaction.commit();
        }
    }

    @Test
    void testChangePropertyDeletedRelationship()
    {
        Node node1 = createNode();
        Node node2 = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode = transaction.getNodeById( node1.getId() );
            var txNode2 = transaction.getNodeById( node2.getId() );

            Relationship rel = txNode.createRelationshipTo( txNode2, MyRelTypes.TEST );
            rel.setProperty( key, 1 );
            rel.delete();
            assertThrows( Exception.class, () ->
            {
                rel.setProperty( key, 2 );
                transaction.commit();
            } );
            txNode.delete();
            txNode2.delete();
            transaction.commit();
        }
    }

    @Test
    void testMultipleDeleteNode()
    {
        Node node1 = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode = transaction.getNodeById( node1.getId() );
            txNode.delete();
            assertThrows( Exception.class, () ->
            {
                txNode.delete();
                transaction.commit();
            } );
        }
    }

    @Test
    void testMultipleDeleteRelationship()
    {
        Node node1 = createNode();
        Node node2 = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode1 = transaction.getNodeById( node1.getId() );
            var txNode2 = transaction.getNodeById( node2.getId() );

            Relationship rel = txNode1.createRelationshipTo( txNode2, MyRelTypes.TEST );
            rel.delete();
            txNode1.delete();
            txNode2.delete();
            assertThrows( Exception.class, () ->
            {
                rel.delete();
                transaction.commit();
            } );
            transaction.commit();
        }
    }

    @Test
    void testIllegalPropertyType()
    {
        Node node1;
        try ( Transaction tx = getGraphDb().beginTx() )
        {
            node1 = tx.createNode();
            assertThrows( Exception.class, () -> node1.setProperty( key, new Object() ) );
        }
    }

    @Test
    void testNodeRelDeleteSemantics()
    {
        Node node1 = createNode();
        Node node2 = createNode();
        Relationship rel1;
        Relationship rel2;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var txNode = transaction.getNodeById( node1.getId() );
            var txNode2 = transaction.getNodeById( node2.getId() );

            rel1 = txNode.createRelationshipTo( txNode2, MyRelTypes.TEST );
            rel2 = txNode.createRelationshipTo( txNode2, MyRelTypes.TEST );
            txNode.setProperty( "key1", "value1" );
            rel1.setProperty( "key1", "value1" );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            var node = transaction.getNodeById( node1.getId() );
            var secondNode = transaction.getNodeById( node2.getId() );
            var relationshipOne = transaction.getRelationshipById( rel1.getId() );
            var relationshipTwo = transaction.getRelationshipById( rel2.getId() );
            node.delete();
            assertThrows( NotFoundException.class, () -> node.getProperty( "key1" ) );
            assertThrows( NotFoundException.class, () -> node.setProperty( "key1", "value2" ) );
            assertThrows( NotFoundException.class, () -> node.removeProperty( "key1" ) );
            secondNode.delete();
            assertThrows( NotFoundException.class, secondNode::delete );
            assertThrows( NotFoundException.class, () -> node.getProperty( "key1" ) );
            assertThrows( NotFoundException.class, () -> node.setProperty( "key1", "value2" ) );
            assertThrows( NotFoundException.class, () -> node.removeProperty( "key1" ) );
            assertEquals( "value1", relationshipOne.getProperty( "key1" ) );
            relationshipOne.delete();
            assertThrows( NotFoundException.class, relationshipOne::delete );
            assertThrows( NotFoundException.class, () -> relationshipOne.getProperty( "key1" ) );
            assertThrows( NotFoundException.class, () -> relationshipOne.setProperty( "key1", "value2" ) );
            assertThrows( NotFoundException.class, () -> relationshipOne.removeProperty( "key1" ) );
            assertThrows( NotFoundException.class, () -> relationshipOne.getProperty( "key1" ) );
            assertThrows( NotFoundException.class, () -> relationshipOne.setProperty( "key1", "value2" ) );
            assertThrows( NotFoundException.class, () -> relationshipOne.removeProperty( "key1" ) );
            assertThrows( NotFoundException.class, () -> secondNode.createRelationshipTo( node, MyRelTypes.TEST ) );
            assertThrows( NotFoundException.class, () -> secondNode.createRelationshipTo( node, MyRelTypes.TEST ) );

            assertEquals( node, relationshipOne.getStartNode() );
            assertEquals( secondNode, relationshipTwo.getEndNode() );
            Node[] nodes = relationshipOne.getNodes();
            assertEquals( node, nodes[0] );
            assertEquals( secondNode, nodes[1] );
            assertEquals( secondNode, relationshipOne.getOtherNode( node ) );
            relationshipTwo.delete();
        }
    }
}
