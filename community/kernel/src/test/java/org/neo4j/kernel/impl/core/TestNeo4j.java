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

import org.junit.Test;

import java.util.Random;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.store.id.IdType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asResourceIterator;

public class TestNeo4j extends AbstractNeo4jTestCase
{
    @Test
    public void testBasicNodeRelationships()
    {
        Node firstNode;
        Node secondNode;
        Relationship rel;
        // Create nodes and a relationship between them
        firstNode = getGraphDb().createNode();
        assertNotNull( "Failure creating first node", firstNode );
        secondNode = getGraphDb().createNode();
        assertNotNull( "Failure creating second node", secondNode );
        rel = firstNode.createRelationshipTo( secondNode, MyRelTypes.TEST );
        assertNotNull( "Relationship is null", rel );
        RelationshipType relType = rel.getType();
        assertNotNull( "Relationship's type is is null", relType );

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
        assertTrue( this.objectExistsInIterable( rel, allRels ) );
        allRels = (ResourceIterable<Relationship>) firstNode.getRelationships( relType );
        assertTrue( this.objectExistsInIterable( rel, allRels ) );

        allRels = (ResourceIterable<Relationship>) secondNode.getRelationships();
        assertTrue( this.objectExistsInIterable( rel, allRels ) );
        allRels = (ResourceIterable<Relationship>) secondNode.getRelationships( relType );
        assertTrue( this.objectExistsInIterable( rel, allRels ) );

        // Verify that the relationship reports that it is associated with
        // firstNode and secondNode
        Node[] relNodes = rel.getNodes();
        assertEquals( "A relationship should always be connected to exactly "
            + "two nodes", relNodes.length, 2 );
        assertTrue( "Relationship says that it isn't connected to firstNode",
            this.objectExistsInArray( firstNode, relNodes ) );
        assertTrue( "Relationship says that it isn't connected to secondNode",
            this.objectExistsInArray( secondNode, relNodes ) );
        assertTrue( "The other node should be secondNode but it isn't", rel
            .getOtherNode( firstNode ).equals( secondNode ) );
        assertTrue( "The other node should be firstNode but it isn't", rel
            .getOtherNode( secondNode ).equals( firstNode ) );
        rel.delete();
        secondNode.delete();
        firstNode.delete();
    }

    private boolean objectExistsInIterable( Relationship rel,
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

    private boolean objectExistsInArray( Object obj, Object[] objArray )
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
    public void testRandomPropertyName()
    {
        Node node1 = getGraphDb().createNode();
        String key = "random_"
            + new Random( System.currentTimeMillis() ).nextLong();
        node1.setProperty( key, "value" );
        assertEquals( "value", node1.getProperty( key ) );
        node1.delete();
    }

    @Test
    public void testNodeChangePropertyArray()
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
    public void testGetAllNodes()
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
    public void testMultipleShutdown()
    {
        commit();
        getGraphDb().shutdown();
        getGraphDb().shutdown();
    }
}
