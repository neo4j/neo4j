/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertTrue;

public class ConcurrentInstanceStartupIT
{
    public static final int INSTANCE_COUNT = 3;
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void concurrentStartupShouldWork() throws Exception
    {
        // Ensures that the instances don't race to create the test's base directory and only care about their own.
        testDirectory.directory( "nothingToSeeHereMoveAlong" );
        StringBuffer initialHostsBuffer = new StringBuffer( "127.0.0.1:5001" );
        for ( int i = 2; i <= INSTANCE_COUNT; i++ )
        {
            initialHostsBuffer.append( ",127.0.0.1:500" + i );
        }
        final String initialHosts = initialHostsBuffer.toString();
        final CyclicBarrier barrier = new CyclicBarrier( INSTANCE_COUNT );
        final List<Thread> daThreads = new ArrayList<Thread>( INSTANCE_COUNT );
        final HighlyAvailableGraphDatabase[] dbs = new HighlyAvailableGraphDatabase[INSTANCE_COUNT];

        for ( int i = 1; i <= INSTANCE_COUNT; i++ )
        {
            final int finalI = i;

            Thread t = new Thread( new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        barrier.await();
                        dbs[finalI - 1] = startDbAtBase( finalI, initialHosts );
                    }
                    catch ( InterruptedException | BrokenBarrierException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            });
            daThreads.add( t );
            t.start();
        }

        for ( Thread daThread : daThreads )
        {
            daThread.join();
        }

        boolean masterDone = false;

        for ( HighlyAvailableGraphDatabase db : dbs )
        {
            if (db.isMaster())
            {
                if (masterDone)
                {
                    throw new Exception("Two masters discovered");
                }
                masterDone = true;
            }
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode();
                tx.success();
            }
        }

        assertTrue( masterDone );

        for ( HighlyAvailableGraphDatabase db : dbs )
        {
            db.shutdown();
        }
    }

    private HighlyAvailableGraphDatabase startDbAtBase( int i, String initialHosts )
    {
        GraphDatabaseBuilder masterBuilder = new TestHighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( path( i ).getAbsoluteFile() )
                .setConfig( ClusterSettings.initial_hosts, initialHosts )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + ( 5000 + i ) )
                .setConfig( ClusterSettings.server_id, "" + i )
                .setConfig( HaSettings.ha_server, ":" + ( 8000 + i ) )
                .setConfig( HaSettings.tx_push_factor, "0" );
        return (HighlyAvailableGraphDatabase) masterBuilder.newGraphDatabase();
    }

    private File path( int i )
    {
        return testDirectory.directory( i + "" );
    }
}
