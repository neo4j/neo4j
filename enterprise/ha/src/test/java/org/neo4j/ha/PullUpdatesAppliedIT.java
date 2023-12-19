/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.test.StreamConsumer;
import org.neo4j.test.rule.TestDirectory;

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
 * bit better than checking debug.log for certain entries. Another, more
 * direct, test is present in community.
 */
public class PullUpdatesAppliedIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    private SortedMap<Integer, Configuration> configurations;
    private Map<Integer, HighlyAvailableGraphDatabase> databases;

    private class Configuration
    {
        final int serverId;
        final int clusterPort;
        final int haPort;
        final File directory;

        Configuration( int serverId, int clusterPort, int haPort, File directory )
        {
            this.serverId = serverId;
            this.clusterPort = clusterPort;
            this.haPort = haPort;
            this.directory = directory;
        }
    }

    @Before
    public void doBefore()
    {
        configurations = createConfigurations();
        databases = startDatabases();
    }

    private SortedMap<Integer, Configuration> createConfigurations()
    {
        SortedMap<Integer, Configuration> configurations = new TreeMap<>();

        IntStream.range( 0, 3 )
                .forEach( serverId ->
                {
                    int clusterPort = PortAuthority.allocatePort();
                    int haPort = PortAuthority.allocatePort();
                    File directory = testDirectory.directory( Integer.toString( serverId ) ).getAbsoluteFile();

                    configurations.put( serverId, new Configuration( serverId, clusterPort, haPort, directory ) );
                } );

        return configurations;
    }

    private Map<Integer, HighlyAvailableGraphDatabase> startDatabases()
    {
        Map<Integer, HighlyAvailableGraphDatabase> databases = new HashMap<>();

        for ( Configuration configuration : configurations.values() )
        {
            int serverId = configuration.serverId;
            int clusterPort = configuration.clusterPort;
            int haPort = configuration.haPort;
            File directory = configuration.directory;

            int initialHostPort = configurations.values().iterator().next().clusterPort;

            HighlyAvailableGraphDatabase hagdb = database( serverId, clusterPort, haPort, directory, initialHostPort );

            databases.put( serverId, hagdb);
        }

        for ( HighlyAvailableGraphDatabase database : databases.values() )
        {
            database.isAvailable( 5000 );
        }

        return databases;
    }

    @After
    public void doAfter()
    {
        if ( databases != null )
        {
            databases.values().stream()
                    .filter( Objects::nonNull )
                    .forEach( GraphDatabaseFacade::shutdown );
        }
    }

    @Test
    public void testUpdatesAreWrittenToLogBeforeBeingAppliedToStore() throws Exception
    {
        int serverIdOfMaster = getCurrentMaster();
        addNode( serverIdOfMaster );
        int serverIdOfDatabaseToKill = findSomeoneNotMaster( serverIdOfMaster );
        HighlyAvailableGraphDatabase databaseToKill = findDatabase( serverIdOfDatabaseToKill );

        final CountDownLatch latch1 = new CountDownLatch( 1 );

        final HighlyAvailableGraphDatabase masterDb = findDatabase( serverIdOfMaster );
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

        databaseToKill.shutdown();

        assertTrue( "Timeout waiting for instance to leave cluster", latch1.await( 60, TimeUnit.SECONDS ) );

        addNode( serverIdOfMaster ); // this will be pulled by tne next start up, applied but not written to log.

        Configuration configuration = configurations.get( serverIdOfDatabaseToKill );

        int clusterPort = configuration.clusterPort;
        int haPort = configuration.haPort;
        File directory = configuration.directory;

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

        runInOtherJvm( directory, serverIdOfDatabaseToKill, clusterPort, haPort, configurations.get( serverIdOfMaster ).clusterPort );

        assertTrue( "Timeout waiting for instance to fail", latch2.await( 60, TimeUnit.SECONDS ) );

        // This is to allow other instances to mark the dead instance as failed, otherwise on startup it will be denied.
        // TODO This is to demonstrate shortcomings in our design. Fix this, you ugly, ugly hacker
        Thread.sleep( 15000 );

        restart( serverIdOfDatabaseToKill ); // recovery and branching.
        boolean hasBranchedData = new File( directory, "branched" ).listFiles().length > 0;
        assertFalse( hasBranchedData );
    }

    private HighlyAvailableGraphDatabase findDatabase( int serverId )
    {
        return databases.get( serverId );
    }

    private int findSomeoneNotMaster( int serverIdOfMaster )
    {
        return databases.keySet().stream()
                .filter( serverId -> serverId != serverIdOfMaster )
                .findAny().orElseThrow( IllegalStateException::new );
    }

    private void restart( int serverId )
    {
        Configuration configuration = configurations.get( serverId );

        int clusterPort = configuration.clusterPort;
        int haPort = configuration.haPort;
        File directory = configuration.directory;

        HighlyAvailableGraphDatabase highlyAvailableGraphDatabase =
                database( serverId, clusterPort, haPort, directory, configurations.values().iterator().next().clusterPort );

        databases.put( serverId, highlyAvailableGraphDatabase );
    }

    private static HighlyAvailableGraphDatabase database( int serverId, int clusterPort, int haPort, File path, int initialHostPort )
    {
        return (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                newEmbeddedDatabaseBuilder( path )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + clusterPort )
                // because we run single threaded: if we specified all 3x cluster members,
                // first database would block, wait, and time out because it would be the only member
                // this makes the test less robust, because it _could_ happen that first instance didn't become or remain master
                .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:" + initialHostPort )
                .setConfig( ClusterSettings.server_id, Integer.toString( serverId ) )
                .setConfig( HaSettings.ha_server, "localhost:" + haPort )
                .setConfig( HaSettings.pull_interval, "0ms" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
                .newGraphDatabase();
    }

    private void runInOtherJvm( File directory, int serverIdOfDatabaseToKill, int clusterPort, int haPort, int initialHostPort ) throws Exception
    {
        List<String> commandLine = new ArrayList<>( Arrays.asList(
                "java",
                "-Djava.awt.headless=true",
                "-cp", System.getProperty( "java.class.path" ),
                PullUpdatesAppliedIT.class.getName() ) );
        commandLine.add( directory.toString() );
        commandLine.add( String.valueOf( serverIdOfDatabaseToKill ) );
        commandLine.add( String.valueOf( clusterPort ) );
        commandLine.add( String.valueOf( haPort ) );
        commandLine.add( String.valueOf( initialHostPort ) );

        Process p = Runtime.getRuntime().exec( commandLine.toArray( new String[commandLine.size()] ) );
        List<Thread> threads = new LinkedList<>();
        launchStreamConsumers( threads, p );
        /*
         * Yes, timeouts suck but HAGD does not terminate politely, since it still has
         * threads running after main() completes, so we need to kill it. When? 10 seconds
         * is good enough.
         */
        // a generous timeout; individual tests' latencies do not matter when running tests in parallel
        Thread.sleep( 30000 );
        p.destroy();
        for ( Thread t : threads )
        {
            t.join();
        }
    }

    // For executing in a different process than the one running the test case.
    public static void main( String[] args ) throws Exception
    {
        File storePath = new File( args[0] );
        int serverId = Integer.parseInt( args[1] );
        int clusterPort = Integer.parseInt( args[2] );
        int haPort = Integer.parseInt( args[3] );
        int initialHostPort = Integer.parseInt( args[4] );

        HighlyAvailableGraphDatabase hagdb = database( serverId, clusterPort, haPort, storePath, initialHostPort );

        hagdb.getDependencyResolver().resolveDependency( UpdatePuller.class ).pullUpdates();
        // this is the bug trigger
        // no shutdown, emulates a crash.
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

    private void addNode( int serverId )
    {
        HighlyAvailableGraphDatabase db = findDatabase( serverId );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().getId();
            tx.success();
        }
    }

    private int getCurrentMaster()
    {
        return databases.entrySet().stream()
                .filter( entry -> entry.getValue().isMaster() )
                .findFirst().orElseThrow( () -> new IllegalStateException( "no master" ) )
                .getKey();
    }
}
