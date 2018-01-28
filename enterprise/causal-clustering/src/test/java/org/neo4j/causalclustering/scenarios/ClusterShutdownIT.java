/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith( Parameterized.class )
public class ClusterShutdownIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 );

    @Parameterized.Parameter()
    public Collection<Integer> shutdownOrder;

    @Parameterized.Parameters( name = "shutdown order {0}" )
    public static Collection<Collection<Integer>> shutdownOrders()
    {
        return asList( asList( 0, 1, 2 ), asList( 1, 2, 0 ), asList( 2, 0, 1 ) );
    }

    @Test
    public void shouldShutdownEvenThoughWaitingForLock() throws Exception
    {
        Cluster cluster = clusterRule.startCluster();

        try
        {
            for ( int victimId = 0; victimId < cluster.numberOfCoreMembersReportedByTopology(); victimId++ )
            {
                assertTrue( cluster.getCoreMemberById( victimId ).database().isAvailable( 1000 ) );
                shouldShutdownEvenThoughWaitingForLock0( cluster, victimId, shutdownOrder );
                cluster.start();
            }
        }
        catch ( WriteOperationsNotAllowedException e )
        {
            // expected
        }
    }

    private void shouldShutdownEvenThoughWaitingForLock0( Cluster cluster, int victimId, Collection<Integer> shutdownOrder )
            throws Exception
    {
        final int LONG_TIME = 60_000;
        final int LONGER_TIME = 2 * LONG_TIME;
        final int NUMBER_OF_LOCK_ACQUIRERS = 2;

        final ExecutorService txExecutor = Executors.newCachedThreadPool(); // Blocking transactions are executed in
        // parallel, not on the main thread.
        final ExecutorService shutdownExecutor = Executors.newFixedThreadPool( 1 ); // Shutdowns are executed
        // serially, not on the main thread.

        try
        {
            // when - blocking on lock acquiring
            final AtomicReference<Node> someNode = new AtomicReference<>();
            final GraphDatabaseService victimDB = cluster.getCoreMemberById( victimId ).database();

            try ( Transaction tx = victimDB.beginTx() )
            {
                someNode.set( victimDB.createNode() );
                tx.success();
            }

            final AtomicInteger numberOfInstancesReady = new AtomicInteger();
            for ( int i = 0; i < NUMBER_OF_LOCK_ACQUIRERS; i++ )
            {
                txExecutor.execute( () ->
                {
                    try ( Transaction tx = victimDB.beginTx() )
                    {
                        numberOfInstancesReady.incrementAndGet();

                        tx.acquireWriteLock( someNode.get() );
                        Thread.sleep( LONGER_TIME );

                        tx.success();
                    }
                    catch ( Exception e )
                    {
                        /* Since we are shutting down, a plethora of possible exceptions are expected. */
                    }
                } );
            }

            while ( numberOfInstancesReady.get() < NUMBER_OF_LOCK_ACQUIRERS )
            {
                Thread.sleep( 100 );
            }

            final CountDownLatch shutdownLatch = new CountDownLatch( cluster.numberOfCoreMembersReportedByTopology() );

            // then - shutdown in any order should still be possible
            for ( final int id : shutdownOrder )
            {
                shutdownExecutor.execute( () ->
                {
                    cluster.getCoreMemberById( id ).shutdown();
                    shutdownLatch.countDown();
                } );
            }

            if ( !shutdownLatch.await( LONG_TIME, MILLISECONDS ) )
            {
                fail( "Cluster didn't shut down in a timely fashion." );
            }
        }
        finally
        {
            txExecutor.shutdownNow();
            shutdownExecutor.shutdownNow();
        }
    }
}
