/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;

@Ignore("This test currently fails by never finishing.")
public class ConcurrentInstanceStartupIT
{
    public static TargetDirectory testDirectory = TargetDirectory.forTest( ConcurrentInstanceStartupIT.class );

    @Test
    public void concurrentStartupShouldWork() throws Exception
    {
        final CyclicBarrier barrier = new CyclicBarrier( 3 );
        final List<Thread> daThreads = new ArrayList<Thread>( 3 );
        final HighlyAvailableGraphDatabase[] dbs = new HighlyAvailableGraphDatabase[3];

        for ( int i = 1; i <= 3; i++ )
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
                        dbs[ finalI-1 ] = startDbAtBase( finalI );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }
                    catch ( BrokenBarrierException e )
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

        for ( HighlyAvailableGraphDatabase db : dbs )
        {
            Transaction tx = db.beginTx();
            db.createNode();
            tx.success(); tx.finish();
        }

        for ( HighlyAvailableGraphDatabase db : dbs )
        {
            db.shutdown();
        }
    }

    private HighlyAvailableGraphDatabase startDbAtBase( int i )
    {
        GraphDatabaseBuilder masterBuilder = new HighlyAvailableGraphDatabaseFactory()
                .newHighlyAvailableDatabaseBuilder( path( i ).getAbsolutePath() )
                .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003" )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + ( 5000 + i ) )
                .setConfig( HaSettings.server_id, "" + i )
                .setConfig( HaSettings.ha_server, ":" + ( 8000 + i ) )
                .setConfig( HaSettings.tx_push_factor, "0" );
        return (HighlyAvailableGraphDatabase) masterBuilder.newGraphDatabase();
    }

    private File path( int i )
    {
        return testDirectory.directory( i+"", true );
    }
}
