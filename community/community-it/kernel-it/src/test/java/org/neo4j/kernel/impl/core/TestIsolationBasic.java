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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestIsolationBasic extends AbstractNeo4jTestCase
{
    /*
     * Tests that changes performed in a transaction before commit are not apparent in another.
     */
    @Test
    void testSimpleTransactionIsolation() throws Exception
    {
        // Start setup - create base data
        final CountDownLatch latch1 = new CountDownLatch( 1 );
        final CountDownLatch latch2 = new CountDownLatch( 1 );
        Node n1;
        Node n2;
        Relationship r1;
        try ( Transaction tx = getGraphDb().beginTx() )
        {
            n1 = tx.createNode();
            n2 = tx.createNode();
            r1 = n1.createRelationshipTo( n2,
                    RelationshipType.withName( "TEST" ) );
            tx.commit();
        }

        final Node node1 = n1;
        final Node node2 = n2;
        final Relationship rel1 = r1;

        try ( Transaction tx = getGraphDb().beginTx() )
        {
            tx.getNodeById( node1.getId() ).setProperty( "key", "old" );
            tx.getRelationshipById( rel1.getId() ).setProperty( "key", "old" );
            tx.commit();
        }
        try ( Transaction tx = getGraphDb().beginTx() )
        {
            var txNode = tx.getNodeById( node1.getId() );
            var txNode2 = tx.getNodeById( node2.getId() );
            var txRel = tx.getRelationshipById( rel1.getId() );
            assertPropertyEqual( txNode, "key", "old" );
            assertPropertyEqual( txRel, "key", "old" );
            assertRelationshipCount( txNode, 1 );
            assertRelationshipCount( txNode2, 1 );
        }

        // This is the mutating transaction - it will change stuff which will be read in between
        final AtomicReference<Exception> t1Exception = new AtomicReference<>();
        Thread t1 = new Thread( () ->
        {
            try ( Transaction tx = getGraphDb().beginTx() )
            {
                var txNode = tx.getNodeById( node1.getId() );
                var txNode2 = tx.getNodeById( node2.getId() );
                var txRel = tx.getRelationshipById( rel1.getId() );
                txNode.setProperty( "key", "new" );
                txRel.setProperty( "key", "new" );
                txNode.createRelationshipTo( txNode2, RelationshipType.withName( "TEST" ) );
                assertPropertyEqual( txNode, "key", "new" );
                assertPropertyEqual( txRel, "key", "new" );
                assertRelationshipCount( txNode, 2 );
                assertRelationshipCount( txNode2, 2 );
                latch1.countDown();
                latch2.await();
                assertPropertyEqual( txNode, "key", "new" );
                assertPropertyEqual( txRel, "key", "new" );
                assertRelationshipCount( txNode, 2 );
                assertRelationshipCount( txNode2, 2 );
                // no tx.success();
            }
            catch ( Exception e )
            {
                Thread.interrupted();
                t1Exception.set( e );
            }
            finally
            {
                try ( Transaction tx = getGraphDb().beginTx() )
                {
                    var txNode = tx.getNodeById( node1.getId() );
                    var txNode2 = tx.getNodeById( node2.getId() );
                    var txRel = tx.getRelationshipById( rel1.getId() );
                    assertPropertyEqual( txNode, "key", "old" );
                    assertPropertyEqual( txRel, "key", "old" );
                    assertRelationshipCount( txNode, 1 );
                    assertRelationshipCount( txNode2, 1 );
                }
                catch ( Exception e )
                {
                    t1Exception.compareAndSet( null, e );
                }
            }
        } );
        t1.start();

        latch1.await();

        // The transaction started above that runs in t1 has not finished. The old values should still be visible.
        try ( Transaction tx = getGraphDb().beginTx() )
        {
            var txNode = tx.getNodeById( node1.getId() );
            var txNode2 = tx.getNodeById( node2.getId() );
            var txRel = tx.getRelationshipById( rel1.getId() );
            assertPropertyEqual( txNode, "key", "old" );
            assertPropertyEqual( txRel, "key", "old" );
            assertRelationshipCount( txNode, 1 );
            assertRelationshipCount( txNode2, 1 );
        }

        latch2.countDown();
        t1.join();

        // The transaction in t1 has finished but not committed. Its changes should still not be visible.
        try ( Transaction tx = getGraphDb().beginTx() )
        {
            var txNode = tx.getNodeById( node1.getId() );
            var txNode2 = tx.getNodeById( node2.getId() );
            var txRel = tx.getRelationshipById( rel1.getId() );
            assertPropertyEqual( txNode, "key", "old" );
            assertPropertyEqual( txRel, "key", "old" );
            assertRelationshipCount( txNode, 1 );
            assertRelationshipCount( txNode2, 1 );
        }
        if ( t1Exception.get() != null )
        {
            throw t1Exception.get();
        }

        try ( Transaction tx = getGraphDb().beginTx() )
        {
            var txNode = tx.getNodeById( node1.getId() );
            var txNode2 = tx.getNodeById( node2.getId() );
            for ( Relationship rel : txNode.getRelationships() )
            {
                rel.delete();
            }
            txNode.delete();
            txNode2.delete();
            tx.commit();
        }
    }

    private void assertPropertyEqual( Entity primitive, String key, String value )
    {
        assertEquals( value, primitive.getProperty( key ) );
    }

    private void assertRelationshipCount( Node node, int count )
    {
        int actualCount = 0;
        for ( Relationship rel : node.getRelationships() )
        {
            actualCount++;
        }
        assertEquals( count, actualCount );
    }
}
