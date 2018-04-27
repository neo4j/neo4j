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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@RunWith( Parameterized.class )
public class ClusterShutdownIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 );

    @Parameterized.Parameter()
    public Collection<Integer> shutdownOrder;
    private Cluster cluster;

    @Parameterized.Parameters( name = "shutdown order {0}" )
    public static Collection<Collection<Integer>> shutdownOrders()
    {
        return asList( asList( 0, 1, 2 ), asList( 1, 2, 0 ), asList( 2, 0, 1 ) );
    }

    @Before
    public void startCluster() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @After
    public void shutdownCluster()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldShutdownEvenThoughWaitingForLock() throws Exception
    {
        CoreClusterMember leader = cluster.awaitLeader();
        shouldShutdownEvenThoughWaitingForLock0( cluster, leader.serverId(), shutdownOrder );
    }

    private void createANode( AtomicReference<Node> node ) throws Exception
    {
        cluster.coreTx( ( coreGraphDatabase, transaction ) ->
        {
            node.set( coreGraphDatabase.createNode() );
            transaction.success();
        } );
    }

    private void shouldShutdownEvenThoughWaitingForLock0( Cluster cluster, int victimId, Collection<Integer> shutdownOrder ) throws Exception
    {
        final int LONG_TIME = 60_000;
        final int NUMBER_OF_LOCK_ACQUIRERS = 2;

        final ExecutorService txExecutor = Executors.newCachedThreadPool(); // Blocking transactions are executed in
        // parallel, not on the main thread.
        final ExecutorService shutdownExecutor = Executors.newFixedThreadPool( 1 ); // Shutdowns are executed
        // serially, not on the main thread.

        final CountDownLatch acquiredLocksCountdown = new CountDownLatch( NUMBER_OF_LOCK_ACQUIRERS );
        final CountDownLatch locksHolder = new CountDownLatch( 1 );
        final AtomicReference<Node> node = new AtomicReference<>();
        final AtomicReference<Exception> txFailure = new AtomicReference<>();

        CompletableFuture<Void> preShutdown = new CompletableFuture<>();

        // set shutdown order
        CompletableFuture<Void> afterShutdown = preShutdown;
        for ( Integer id : shutdownOrder )
        {
            afterShutdown = afterShutdown.thenRunAsync( () -> cluster.getCoreMemberById( id ).shutdown(), shutdownExecutor );
        }

        createANode( node );

        try
        {
            // when - blocking on lock acquiring
            final GraphDatabaseService leader = cluster.getCoreMemberById( victimId ).database();

            for ( int i = 0; i < NUMBER_OF_LOCK_ACQUIRERS; i++ )
            {
                txExecutor.execute( () ->
                {
                    try ( Transaction tx = leader.beginTx() )
                    {
                        tx.acquireWriteLock( node.get() );
                        acquiredLocksCountdown.countDown();
                        locksHolder.await();
                        tx.success();
                    }
                    catch ( Exception e )
                    {
                        txFailure.accumulateAndGet( e, ( e1, e2 ) ->
                        {
                            if ( e1 != null && e2 != null && !e1.equals( e2 ) )
                            {
                                e1.addSuppressed( e2 );
                                return e1;
                            }
                            return e2;
                        } );
                    }
                } );
            }

            // await locks
            acquiredLocksCountdown.await( LONG_TIME, MILLISECONDS );

            // check for premature failures
            Thread.sleep( 100 );
            Exception prematureFailure = txFailure.get();
            if ( prematureFailure != null )
            {
                throw new RuntimeException( "Failed prematurely", prematureFailure );
            }

            // then shutdown in given order works
            preShutdown.complete( null );
            afterShutdown.get( LONG_TIME, MILLISECONDS );
        }
        finally
        {
            afterShutdown.cancel( true );
            locksHolder.countDown();
            txExecutor.shutdownNow();
            shutdownExecutor.shutdownNow();
        }
    }
}
