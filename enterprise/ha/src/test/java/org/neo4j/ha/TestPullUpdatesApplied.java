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
package org.neo4j.ha;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.test.StreamConsumer;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test case ensures that updates in HA are first written out to the log
 * and then applied to the store. The problem appears after recovering an
 * unclean shutdown of a slave where no transactions happened (hence the log
 * buffer was not forced). Then it will try to retrieve the latest tx (as
 * written in neostore) from its logs but it will not be present there. This
 * will throw the exception of being unable to find the commit entry for that
 * txid and that will lead to branching. The exception is thrown during startup,
 * before the constructor returns, so we cannot test from userland. Instead we
 * check for the symptom, which is the branched store. This is not nice, just a
 * bit better than checking messages.log for certain entries. Another, more
 * direct, test is present in community.
 */
public class TestPullUpdatesApplied
{
    private final HighlyAvailableGraphDatabase[] dbs = new HighlyAvailableGraphDatabase[3];
    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    @Before
    public void doBefore() throws Exception
    {
        for ( int i = 0; i < dbs.length; i++ )
        {
            dbs[i] = newDb( i );
        }

        // Wait for all db's to become available
        for ( HighlyAvailableGraphDatabase db : dbs )
        {
            db.isAvailable( 5000 );
        }
    }

    @After
    public void doAfter() throws Exception
    {
        for ( HighlyAvailableGraphDatabase db : dbs )
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    @Test
    public void testUpdatesAreWrittenToLogBeforeBeingAppliedToStore() throws Exception
    {
        int master = getCurrentMaster();
        addNode( master );
        int toKill = (master + 1) % dbs.length;
        HighlyAvailableGraphDatabase dbToKill = dbs[toKill];

        final CountDownLatch latch1 = new CountDownLatch( 1 );

        final HighlyAvailableGraphDatabase masterDb = dbs[master];
        masterDb.getDependencyResolver().resolveDependency( ClusterClient.class ).addClusterListener(
                new ClusterListener.Adapter()
                {
                    @Override
                    public void leftCluster( InstanceId instanceId, URI member )
                    {
                        latch1.countDown();
                        masterDb.getDependencyResolver().resolveDependency( ClusterClient.class )
                                .removeClusterListener( this );
                    }
                } );

        dbToKill.shutdown();

        assertTrue( "Timeout waiting for instance to leave cluster", latch1.await( 60, TimeUnit.SECONDS ) );

        addNode( master ); // this will be pulled by tne next start up, applied
        // but not written to log.
        File targetDirectory = testDirectory.directory( "" + toKill );

        // Setup to detect shutdown of separate JVM, required since we don't exit cleanly. That is also why we go
        // through the heartbeat and not through the cluster change as above.
        final CountDownLatch latch2 = new CountDownLatch( 1 );

        masterDb.getDependencyResolver().resolveDependency( ClusterClient.class ).addHeartbeatListener(
                new HeartbeatListener.Adapter()
                {
                    @Override
                    public void failed( InstanceId server )
                    {
                        latch2.countDown();
                        masterDb.getDependencyResolver().resolveDependency( ClusterClient.class )
                                .removeHeartbeatListener( this );
                    }
                } );

        runInOtherJvmToGetExitCode( targetDirectory.getAbsolutePath(), "" + toKill );

        assertTrue( "Timeout waiting for instance to fail", latch2.await( 60, TimeUnit.SECONDS ) );

        // This is to allow other instances to mark the dead instance as failed, otherwise on startup it will be denied.
        // TODO This is to demonstrate shortcomings in our design. Fix this, you ugly, ugly hacker
        Thread.sleep( 15000 );

        restart( toKill ); // recovery and branching.
        boolean hasBranchedData = new File( targetDirectory, "branched" ).listFiles().length > 0;
        assertFalse( hasBranchedData );
    }

    // For executing in a different process than the one running the test case.
    public static void main( String[] args ) throws Exception
    {
        String storePath = args[0];
        int serverId = Integer.parseInt( args[1] );

        database( serverId, storePath ).getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
        // this is the bug trigger
        // no shutdown, emulates a crash.
    }

    private HighlyAvailableGraphDatabase newDb( int serverId )
    {
        return database( serverId, testDirectory.directory( Integer.toString( serverId ) ).getAbsolutePath() );
    }

    private void restart( int serverId )
    {
        dbs[serverId] = database( serverId, testDirectory.directory( Integer.toString( serverId ) ).getAbsolutePath() );
    }

    private static HighlyAvailableGraphDatabase database( int serverId, String path )
    {
        return (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( path )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + (5001 + serverId) )
                .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                .setConfig( ClusterSettings.server_id, Integer.toString( serverId ) )
                .setConfig( HaSettings.ha_server, "localhost:" + (6666 + serverId) )
                .setConfig( HaSettings.pull_interval, "0ms" )
                .newGraphDatabase();
    }

    private static int runInOtherJvmToGetExitCode( String... args ) throws Exception
    {
        List<String> allArgs = new ArrayList<String>( Arrays.asList( "java", "-Djava.awt.headless=true", "-cp",
                System.getProperty( "java.class.path" ), TestPullUpdatesApplied.class.getName() ) );
        allArgs.addAll( Arrays.asList( args ) );
        Process p = Runtime.getRuntime().exec( allArgs.toArray( new String[allArgs.size()] ) );
        List<Thread> threads = new LinkedList<>();
        launchStreamConsumers( threads, p );
        /*
         * Yes, timeouts suck but HAGD does not terminate politely, since it still has
         * threads running after main() completes, so we need to kill it. When? 10 seconds
         * is good enough.
         */
        Thread.sleep( 10000 );
        p.destroy();
        for ( Thread t : threads )
        {
            t.join();
        }
        return 0;
    }

    private static void launchStreamConsumers( List<Thread> join, Process p )
    {
        InputStream outStr = p.getInputStream();
        InputStream errStr = p.getErrorStream();
        Thread out = new Thread( new StreamConsumer( outStr, System.out, false ) );
        join.add( out );
        Thread err = new Thread( new StreamConsumer( errStr, System.err, false ) );
        join.add( err );
        out.start();
        err.start();
    }

    private long addNode( int dbId )
    {
        HighlyAvailableGraphDatabase db = dbs[dbId];
        long result = -1;
        try ( Transaction tx = db.beginTx() )
        {
            result = db.createNode().getId();
            tx.success();
        }
        return result;
    }

    private int getCurrentMaster() throws Exception
    {
        for ( int i = 0; i < dbs.length; i++ )
        {
            HighlyAvailableGraphDatabase db = dbs[i];
            if ( db.isMaster() )
            {
                return i;
            }
        }
        return -1;
    }
}
