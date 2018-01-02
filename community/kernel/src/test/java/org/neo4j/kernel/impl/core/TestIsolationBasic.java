/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

import static org.junit.Assert.*;

public class TestIsolationBasic extends AbstractNeo4jTestCase
{
    /*
     * Tests that changes performed in a transaction before commit are not apparent in another.
     */
    @Test
    public void testSimpleTransactionIsolation() throws Exception
    {
        // Start setup - create base data
        commit();
        final CountDownLatch latch1 = new CountDownLatch( 1 );
        final CountDownLatch latch2 = new CountDownLatch( 1 );
        Node n1, n2;
        Relationship r1;
        try ( Transaction tx = getGraphDb().beginTx() )
        {
            n1 = getGraphDb().createNode();
            n2 = getGraphDb().createNode();
            r1 = n1.createRelationshipTo( n2, 
                DynamicRelationshipType.withName( "TEST" ) );
            tx.success();
        }

        final Node node1 = n1;
        final Node node2 = n2;
        final Relationship rel1 = r1;

        try (Transaction tx = getGraphDb().beginTx())
        {
            node1.setProperty( "key", "old" );
            rel1.setProperty( "key", "old" );
            tx.success();
        }
        assertPropertyEqual( node1, "key", "old" );
        assertPropertyEqual( rel1, "key", "old" );
        assertRelationshipCount( node1, 1 );
        assertRelationshipCount( node2, 1 );

        // This is the mutating transaction - it will change stuff which will be read in between
        final AtomicReference<Exception> t1Exception = new AtomicReference<>();
        Thread t1 = new Thread( new Runnable()
        {
            public void run()
            {

                try(Transaction tx = getGraphDb().beginTx())
                {
                    node1.setProperty( "key", "new" );
                    rel1.setProperty( "key", "new" );
                    node1.createRelationshipTo( node2, 
                        DynamicRelationshipType.withName( "TEST" ) );
                    assertPropertyEqual( node1, "key", "new" );
                    assertPropertyEqual( rel1, "key", "new" );
                    assertRelationshipCount( node1, 2 );
                    assertRelationshipCount( node2, 2 );
                    latch1.countDown();
                    latch2.await();
                    assertPropertyEqual( node1, "key", "new" );
                    assertPropertyEqual( rel1, "key", "new" );
                    assertRelationshipCount( node1, 2 );
                    assertRelationshipCount( node2, 2 );
                    // no tx.success();
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    Thread.interrupted();
                    t1Exception.set( e );
                }
                finally
                {
                    try
                    {
                        assertPropertyEqual( node1, "key", "old" );
                        assertPropertyEqual( rel1, "key", "old" );
                        assertRelationshipCount( node1, 1 );
                        assertRelationshipCount( node2, 1 );
                    }
                    catch(Exception e)
                    {
                        t1Exception.compareAndSet( null, e );
                    }
                }
            }
        } );
        t1.start();

        latch1.await();

        // The transaction started above that runs in t1 has not finished. The old values should still be visible.
        assertPropertyEqual( node1, "key", "old" );
        assertPropertyEqual( rel1, "key", "old" );
        assertRelationshipCount( node1, 1 );
        assertRelationshipCount( node2, 1 );

        latch2.countDown();
        t1.join();

        // The transaction in t1 has finished but not committed. Its changes should still not be visible.
        assertPropertyEqual( node1, "key", "old" );
        assertPropertyEqual( rel1, "key", "old" );
        assertRelationshipCount( node1, 1 );
        assertRelationshipCount( node2, 1 );

        if(t1Exception.get() != null)
        {
            throw t1Exception.get();
        }


        try (Transaction tx = getGraphDb().beginTx())
        {
            for ( Relationship rel : node1.getRelationships() )
            {
                rel.delete();
            }
            node1.delete();
            node2.delete();
            tx.success();
        }
    }
    
    private void assertPropertyEqual( PropertyContainer primitive, String key, 
        String value )
    {
        try ( Transaction tx = getGraphDb().beginTx() )
        {
            assertEquals( value, primitive.getProperty( key ) );
        }
    }
    
    private void assertRelationshipCount( Node node, int count )
    {

        try ( Transaction tx = getGraphDb().beginTx() )
        {
            int actualCount = 0;
            for ( Relationship rel : node.getRelationships() )
            {
                actualCount++;
            }
            assertEquals( count, actualCount );
        }
    }
}
