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

import java.util.Random;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdType;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.MyRelTypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;

class TestNeo4j extends AbstractNeo4jTestCase
{
    @Test
    void testBasicNodeRelationships()
    {
        Node firstNode;
        Node secondNode;
        Relationship rel;
        // Create nodes and a relationship between them
        firstNode = getGraphDb().createNode();
        assertNotNull( firstNode, "Failure creating first node" );
        secondNode = getGraphDb().createNode();
        assertNotNull( secondNode, "Failure creating second node" );
        rel = firstNode.createRelationshipTo( secondNode, MyRelTypes.TEST );
        assertNotNull( rel, "Relationship is null" );
        RelationshipType relType = rel.getType();
        assertNotNull( relType, "Relationship's type is is null" );

        // Verify that the node reports that it has a relationship of
        // the type we created above
        try ( ResourceIterator<Relationship> iterator = asResourceIterator( firstNode.getRelationships( relType ).iterator() ) )
        {
            assertTrue( iterator.hasNext() );
        }
        try ( ResourceIterator<Relationship> iterator = asResourceIterator( secondNode.getRelationships( relType ).iterator() ) )
        {
            assertTrue( iterator.hasNext() );
        }

        ResourceIterable<Relationship> allRels;

        // Verify that both nodes return the relationship we created above
        allRels = (ResourceIterable<Relationship>) firstNode.getRelationships();
        assertTrue( objectExistsInIterable( rel, allRels ) );
        allRels = (ResourceIterable<Relationship>) firstNode.getRelationships( relType );
        assertTrue( objectExistsInIterable( rel, allRels ) );

        allRels = (ResourceIterable<Relationship>) secondNode.getRelationships();
        assertTrue( objectExistsInIterable( rel, allRels ) );
        allRels = (ResourceIterable<Relationship>) secondNode.getRelationships( relType );
        assertTrue( objectExistsInIterable( rel, allRels ) );

        // Verify that the relationship reports that it is associated with
        // firstNode and secondNode
        Node[] relNodes = rel.getNodes();
        assertEquals( relNodes.length, 2, "A relationship should always be connected to exactly two nodes" );
        assertTrue( objectExistsInArray( firstNode, relNodes ), "Relationship says that it isn't connected to firstNode" );
        assertTrue( objectExistsInArray( secondNode, relNodes ), "Relationship says that it isn't connected to secondNode" );
        assertEquals( rel.getOtherNode( firstNode ), secondNode, "The other node should be secondNode but it isn't" );
        assertEquals( rel.getOtherNode( secondNode ), firstNode, "The other node should be firstNode but it isn't" );
        rel.delete();
        secondNode.delete();
        firstNode.delete();
    }

    private static boolean objectExistsInIterable( Relationship rel,
        ResourceIterable<Relationship> allRels )
    {
        try ( ResourceIterator<Relationship> resourceIterator = allRels.iterator() )
        {
            while ( resourceIterator.hasNext() )
            {
                Relationship iteratedRel = resourceIterator.next();
                {
                    if ( rel.equals( iteratedRel ) )
                    {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static boolean objectExistsInArray( Object obj, Object[] objArray )
    {
        for ( Object o : objArray )
        {
            if ( o.equals( obj ) )
            {
                return true;
            }
        }
        return false;
    }

    @Test
    void testRandomPropertyName()
    {
        Node node1 = getGraphDb().createNode();
        String key = "random_"
            + new Random( System.currentTimeMillis() ).nextLong();
        node1.setProperty( key, "value" );
        assertEquals( "value", node1.getProperty( key ) );
        node1.delete();
    }

    @Test
    void testNodeChangePropertyArray()
    {
        getTransaction().close();

        Node node;
        try ( Transaction tx = getGraphDb().beginTx() )
        {
            node = getGraphDb().createNode();
            tx.success();
        }

        try ( Transaction tx = getGraphDb().beginTx() )
        {
            node.setProperty( "test", new String[] { "value1" } );
            tx.success();
        }

        try ( Transaction ignored = getGraphDb().beginTx() )
        {
            node.setProperty( "test", new String[] { "value1", "value2" } );
            // no success, we wanna test rollback on this operation
        }

        try ( Transaction tx = getGraphDb().beginTx() )
        {
            String[] value = (String[]) node.getProperty( "test" );
            assertEquals( 1, value.length );
            assertEquals( "value1", value[0] );
            tx.success();
        }

        setTransaction( getGraphDb().beginTx() );
    }

    @Test
    void testGetAllNodes()
    {
        long highId = getIdGenerator( IdType.NODE ).getHighestPossibleIdInUse();
        if ( highId >= 0 && highId < 10000 )
        {
            long count = Iterables.count( getGraphDb().getAllNodes() );
            boolean found = false;
            Node newNode = getGraphDb().createNode();
            newTransaction();
            long oldCount = count;
            count = 0;
            for ( Node node : getGraphDb().getAllNodes() )
            {
                count++;
                if ( node.equals( newNode ) )
                {
                    found = true;
                }
            }
            assertTrue( found );
            assertEquals( count, oldCount + 1 );

            // Tests a bug in the "all nodes" iterator
            ResourceIterator<Node> allNodesIterator = getGraphDb().getAllNodes().iterator();
            assertNotNull( allNodesIterator.next() );
            allNodesIterator.close();

            newNode.delete();
            newTransaction();
            found = false;
            count = 0;
            for ( Node node : getGraphDb().getAllNodes() )
            {
                count++;
                if ( node.equals( newNode ) )
                {
                    found = true;
                }
            }
            assertTrue( !found );
            assertEquals( count, oldCount );
        }
        // else we skip test, takes too long
    }

    @Test
    void testMultipleShutdown()
    {
        commit();
        getManagementService().shutdown();
        getManagementService().shutdown();
    }
}
