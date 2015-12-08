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

import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TargetDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.Assert.assertEventually;

@Ignore("Currently we only support writing on the leader")
public class ConcurrentLockingIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;

    @After
    public void shutdown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldConcurrentlyLock() throws Exception
    {
        final int coreServerCount = 5;
        final int workersPerCorServer = 5;
        final long nodeCount = 10;
        final long durationMillis = 20_000;

        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, coreServerCount, 0);

        ExecutorService executor = Executors.newFixedThreadPool( coreServerCount * workersPerCorServer );

        createNodes( nodeCount );

        ArrayList<Callable<Long>> workerThreads = new ArrayList<>();

        // when
        for ( final CoreGraphDatabase coreDB : cluster.coreServers() )
        {
            for ( int i = 0; i < workersPerCorServer; i++ )
            {
                Callable<Long> workedThread = new Callable<Long>()
                {
                    ThreadLocalRandom tlr = ThreadLocalRandom.current();
                    long count = 0;

                    @Override
                    public Long call() throws Exception
                    {
                        try
                        {
                            long endTime = System.currentTimeMillis() + durationMillis;

                            while ( System.currentTimeMillis() <= endTime )
                            {
                                try ( Transaction tx = coreDB.beginTx() )
                                {
                                    for ( int i = 0; i < nodeCount; i++ )
                                    {
                                        Node node = coreDB.getNodeById( i );

                                        if ( tlr.nextBoolean() )
                                        {
                                            tx.acquireReadLock( node );
                                            count++;
                                        }
                                        else
                                        {
                                            tx.acquireWriteLock( node );
                                            count++;
                                        }
                                    }

                                    tx.success();
                                }
                                catch ( RuntimeException e )
                                {
                                    e.printStackTrace();
                                    LockSupport.parkNanos( 250_000_000 );
                                }
                            }
                        }
                        catch ( Exception e )
                        {
                            e.printStackTrace();
                        }

                        return count;
                    }
                };

                workerThreads.add( workedThread );
            }
        }

        List<Future<Long>> futures = executor.invokeAll( workerThreads );
        for ( Future<Long> future : futures )
        {
            assertThat( future.get(), greaterThan( 0L ) );
        }
        executor.shutdownNow();
        executor.awaitTermination( durationMillis + 5_000, TimeUnit.MILLISECONDS );
    }

    public void createNodes( long nodeCount ) throws Exception
    {
        GraphDatabaseService coreServer = cluster.findLeader( 5000 );
        try ( Transaction tx = coreServer.beginTx() )
        {
            for ( int i = 0; i < nodeCount; i++ )
            {
                coreServer.createNode();
                tx.success();
            }
        }

        for ( final CoreGraphDatabase db : cluster.coreServers() )
        {
            try ( Transaction tx = db.beginTx() )
            {
                ThrowingSupplier<Long, Exception> actualNodeCount = () -> count( db.getAllNodes() );

                assertEventually( "nodes to appear", actualNodeCount, is( nodeCount ), 10, SECONDS );
                tx.success();
            }
        }
    }
}
