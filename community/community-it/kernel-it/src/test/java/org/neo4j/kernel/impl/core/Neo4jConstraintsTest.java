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
            for ( Node node : getGraphDb().getAllNodes() )
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
            assertFalse( getGraphDb().getAllNodes().iterator().hasNext() );
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
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            node1.delete();
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
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            node2.delete();
            node1.delete();
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
            Relationship rel0 = node0.createRelationshipTo( node1, MyRelTypes.TEST );
            rel1 = node0.createRelationshipTo( node2, MyRelTypes.TEST );
            node1.delete();
            rel0.delete();
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node2.delete();
            rel1.delete();
            node0.delete();
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
            node1.delete();
            assertThrows( Exception.class, () -> node1.createRelationshipTo( node2, MyRelTypes.TEST ) );
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node2.delete();
            node1.delete();
            transaction.commit();
        }
    }

    @Test
    void testAddPropertyDeletedNode()
    {
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.delete();
            assertThrows( Exception.class, () -> node.setProperty( key, 1 ) );
        }
    }

    @Test
    void testRemovePropertyDeletedNode()
    {
        GraphDatabaseService database = getGraphDb();
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode();
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
            node.setProperty( key, 1 );
            node.delete();
            assertThrows( Exception.class, () ->
            {
                node.setProperty( key, 2 );
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
            Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
            rel.delete();
            assertThrows( Exception.class, () ->
            {
                rel.setProperty( key, 1 );
                transaction.commit();
            } );
            node1.delete();
            node2.delete();
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
            Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
            rel.setProperty( key, 1 );
            rel.delete();
            assertThrows( Exception.class, () ->
            {
                rel.removeProperty( key );
                transaction.commit();
            } );
            node1.delete();
            node2.delete();
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
            Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
            rel.setProperty( key, 1 );
            rel.delete();
            assertThrows( Exception.class, () ->
            {
                rel.setProperty( key, 2 );
                transaction.commit();
            } );
            node1.delete();
            node2.delete();
            transaction.commit();
        }
    }

    @Test
    void testMultipleDeleteNode()
    {
        Node node1 = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node1.delete();
            assertThrows( Exception.class, () ->
            {
                node1.delete();
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
            Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
            rel.delete();
            node1.delete();
            node2.delete();
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
            node1 = getGraphDb().createNode();
            assertThrows( Exception.class, () -> node1.setProperty( key, new Object() ) );
        }
        Node node3 = createNode();
        Node node2 = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Relationship rel = node3.createRelationshipTo( node2, MyRelTypes.TEST );
            assertThrows( NotFoundException.class, () -> getGraphDb().getNodeById( node1.getId() ) );
            assertThrows( Exception.class, () -> rel.setProperty( key, new Object() ) );
            assertThrows( Exception.class, transaction::commit );
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
            rel1 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
            rel2 = node1.createRelationshipTo( node2, MyRelTypes.TEST );
            node1.setProperty( "key1", "value1" );
            rel1.setProperty( "key1", "value1" );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node1.delete();
            assertThrows( NotFoundException.class, () -> node1.getProperty( "key1" ) );
            assertThrows( NotFoundException.class, () -> node1.setProperty( "key1", "value2" ) );
            assertThrows( NotFoundException.class, () -> node1.removeProperty( "key1" ) );
            node2.delete();
            assertThrows( NotFoundException.class, node2::delete );
            assertThrows( NotFoundException.class, () -> node1.getProperty( "key1" ) );
            assertThrows( NotFoundException.class, () -> node1.setProperty( "key1", "value2" ) );
            assertThrows( NotFoundException.class, () -> node1.removeProperty( "key1" ) );
            assertEquals( "value1", rel1.getProperty( "key1" ) );
            rel1.delete();
            assertThrows( NotFoundException.class, rel1::delete );
            assertThrows( NotFoundException.class, () -> rel1.getProperty( "key1" ) );
            assertThrows( NotFoundException.class, () -> rel1.setProperty( "key1", "value2" ) );
            assertThrows( NotFoundException.class, () -> rel1.removeProperty( "key1" ) );
            assertThrows( NotFoundException.class, () -> rel1.getProperty( "key1" ) );
            assertThrows( NotFoundException.class, () -> rel1.setProperty( "key1", "value2" ) );
            assertThrows( NotFoundException.class, () -> rel1.removeProperty( "key1" ) );
            assertThrows( NotFoundException.class, () -> node2.createRelationshipTo( node1, MyRelTypes.TEST ) );
            assertThrows( NotFoundException.class, () -> node2.createRelationshipTo( node1, MyRelTypes.TEST ) );

            assertEquals( node1, rel1.getStartNode() );
            assertEquals( node2, rel2.getEndNode() );
            Node[] nodes = rel1.getNodes();
            assertEquals( node1, nodes[0] );
            assertEquals( node2, nodes[1] );
            assertEquals( node2, rel1.getOtherNode( node1 ) );
            rel2.delete();
        }
    }
}
