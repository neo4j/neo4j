/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.scenarios;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.TestOnlyDiscoveryServiceFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TargetDirectory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static junit.framework.TestCase.fail;

@Ignore
public class ClusterShutdownIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldShutdownEvenThoughWaitingForLock() throws Exception
    {
        final int clusterSize = 3;

        // We test a reasonable set of permutations.
        int shutdownOrders[][] = {{0, 1, 2}, {1, 2, 0}, {2, 0, 1}};

        for ( int victimId = 0; victimId < clusterSize; victimId++ )
        {
            for ( int[] shutdownOrder : shutdownOrders )
            {
                shouldShutdownEvenThoughWaitingForLock0( clusterSize, victimId, shutdownOrder );
            }
        }
    }

    private void shouldShutdownEvenThoughWaitingForLock0( int clusterSize, int victimId, int[] shutdownOrder ) throws
            Exception
    {
        final int LONG_TIME = 60_000;
        final int LONGER_TIME = 2 * LONG_TIME;
        final int NUMBER_OF_LOCK_ACQUIRERS = 2;

        final ExecutorService txExecutor = Executors.newCachedThreadPool(); // Blocking transactions are executed in
        // parallel, not on the main thread.
        final ExecutorService shutdownExecutor = Executors.newFixedThreadPool( 1 ); // Shutdowns are executed
        // serially, not on the main thread.

        final File dbDir = dir.directory();

        // given - a cluster
        final Cluster cluster = Cluster.start( dbDir, clusterSize, 0, new TestOnlyDiscoveryServiceFactory() );

        try
        {
            // when - blocking on lock acquiring
            final AtomicReference<Node> someNode = new AtomicReference<>();
            final GraphDatabaseService victimDB = cluster.getCoreServerById( victimId );

            try ( Transaction tx = victimDB.beginTx() )
            {
                someNode.set( victimDB.createNode() );
                tx.success();
            }

            final AtomicInteger numberOfInstancesReady = new AtomicInteger();
            for ( int i = 0; i < NUMBER_OF_LOCK_ACQUIRERS; i++ )
            {
                txExecutor.execute( new Runnable()
                {
                    @Override
                    public void run()
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
                    }
                } );
            }

            while ( numberOfInstancesReady.get() < NUMBER_OF_LOCK_ACQUIRERS )
            {
                Thread.sleep( 100 );
            }

            final CountDownLatch shutdownLatch = new CountDownLatch( clusterSize );

            // then - shutdown in any order should still be possible
            for ( final Integer id : shutdownOrder )
            {
                shutdownExecutor.execute( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        cluster.getCoreServerById( id ).shutdown();
                        shutdownLatch.countDown();
                    }
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

            cluster.shutdown();
        }
    }
}
