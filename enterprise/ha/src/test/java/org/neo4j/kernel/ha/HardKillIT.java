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
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.member.paxos.MemberIsAvailable;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.SLAVE;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.UNKNOWN;

public class HardKillIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    private ProcessStreamHandler processHandler;

    @Test
    public void testMasterSwitchHappensOnKillMinus9() throws Exception
    {
        Process proc = null;
        HighlyAvailableGraphDatabase dbWithId2 = null;
        HighlyAvailableGraphDatabase dbWithId3 = null;
        HighlyAvailableGraphDatabase oldMaster = null;

        int clusterPort1 = PortAuthority.allocatePort();
        int clusterPort2 = PortAuthority.allocatePort();
        int clusterPort3 = PortAuthority.allocatePort();

        int haPort1 = PortAuthority.allocatePort();
        int haPort2 = PortAuthority.allocatePort();
        int haPort3 = PortAuthority.allocatePort();

        try
        {
            proc = run( 1, clusterPort1, haPort1, clusterPort1, clusterPort2 );
            dbWithId2 = startDb( 2, path( 2 ), clusterPort2, haPort2, clusterPort1, clusterPort2 );
            dbWithId3 = startDb( 3, path( 3 ), clusterPort3, haPort3, clusterPort1, clusterPort2 );

            waitUntilAllProperlyAvailable( dbWithId2, 1, MASTER, 2, SLAVE, 3, SLAVE );
            waitUntilAllProperlyAvailable( dbWithId3, 1, MASTER, 2, SLAVE, 3, SLAVE );

            final CountDownLatch newMasterAvailableLatch = new CountDownLatch( 1 );
            dbWithId2.getDependencyResolver().resolveDependency( ClusterClient.class ).addAtomicBroadcastListener(
                    value ->
                    {
                        try
                        {
                            Object event = new AtomicBroadcastSerializer( new ObjectStreamFactory(), new
                                    ObjectStreamFactory() ).receive( value );
                            if ( event instanceof MemberIsAvailable )
                            {
                                if ( HighAvailabilityModeSwitcher.MASTER.equals( ((MemberIsAvailable) event)
                                        .getRole() ) )
                                {
                                    newMasterAvailableLatch.countDown();
                                }
                            }
                        }
                        catch ( Exception e )
                        {
                            fail( e.toString() );
                        }
                    } );
            proc.destroy();
            proc = null;

            assertTrue( newMasterAvailableLatch.await( 60, SECONDS ) );

            assertTrue( dbWithId2.isMaster() );
            assertTrue( !dbWithId3.isMaster() );

            // Ensure that everyone has marked the killed instance as failed, otherwise it cannot rejoin
            waitUntilAllProperlyAvailable( dbWithId2, 1, UNKNOWN, 2, MASTER, 3, SLAVE );
            waitUntilAllProperlyAvailable( dbWithId3, 1, UNKNOWN, 2, MASTER, 3, SLAVE );

            oldMaster = startDb( 1, path( 1 ), clusterPort1, haPort1, clusterPort1, clusterPort2 );
            long oldMasterNode = createNamedNode( oldMaster, "Old master" );
            assertEquals( oldMasterNode, getNamedNode( dbWithId2, "Old master" ) );
        }
        finally
        {
            if ( proc != null )
            {
                proc.destroy();
            }
            if ( oldMaster != null )
            {
                oldMaster.shutdown();
            }
            if ( dbWithId2 != null )
            {
                dbWithId2.shutdown();
            }
            if ( dbWithId3 != null )
            {
                dbWithId3.shutdown();
            }
        }
    }

    private long getNamedNode( HighlyAvailableGraphDatabase db, String name )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                if ( name.equals( node.getProperty( "name", null ) ) )
                {
                    return node.getId();
                }
            }
            fail( "Couldn't find named node '" + name + "' at " + db );
            // The lone above will prevent this return from happening
            return -1;
        }
    }

    private long createNamedNode( HighlyAvailableGraphDatabase db, String name )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "name", name );
            tx.success();
            return node.getId();
        }
    }

    private Process run( int machineId, int clusterPort, int haPort, int initialHost1, int initialHost2 ) throws IOException
    {
        List<String> allArgs = new ArrayList<>( Arrays.asList( "java", "-cp",
                System.getProperty( "java.class.path" ), "-Djava.awt.headless=true", HardKillIT.class.getName() ) );
        allArgs.add( "" + machineId );
        allArgs.add( path( machineId ).getAbsolutePath() );
        allArgs.add( String.valueOf( clusterPort ) );
        allArgs.add( String.valueOf( haPort ) );
        allArgs.add( String.valueOf( initialHost1 ) );
        allArgs.add( String.valueOf( initialHost2 ) );

        Process process = Runtime.getRuntime().exec( allArgs.toArray( new String[allArgs.size()] ) );
        processHandler = new ProcessStreamHandler( process, false );
        processHandler.launch();
        return process;
    }

    /*
     * Used to launch the master instance
     */
    public static void main( String[] args ) throws InterruptedException
    {
        int serverId = Integer.parseInt( args[0] );
        File path = new File( args[1] );
        int clusterPort = Integer.parseInt( args[2] );
        int haPort = Integer.parseInt( args[3] );
        int initialHost1 = Integer.parseInt( args[4] );
        int initialHost2 = Integer.parseInt( args[5] );
        HighlyAvailableGraphDatabase db = startDb( serverId, path, clusterPort, haPort, initialHost1, initialHost2 );
        waitUntilAllProperlyAvailable( db, 1, MASTER, 2, SLAVE, 3, SLAVE );
    }

    private static void waitUntilAllProperlyAvailable( HighlyAvailableGraphDatabase db, Object... expected )
            throws InterruptedException
    {
        ClusterMembers members = db.getDependencyResolver().resolveDependency( ClusterMembers.class );
        long endTime = currentTimeMillis() + SECONDS.toMillis( 120 );
        Map<Integer,String> expectedStates = toExpectedStatesMap( expected );
        while ( currentTimeMillis() < endTime && !allMembersAreAsExpected( members, expectedStates ) )
        {
            Thread.sleep( 100 );
        }
        if ( !allMembersAreAsExpected( members, expectedStates ) )
        {
            throw new IllegalStateException( "Not all members available, according to " + db );
        }
    }

    private static boolean allMembersAreAsExpected( ClusterMembers members, Map<Integer,String> expectedStates )
    {
        int count = 0;
        for ( ClusterMember member : members.getMembers() )
        {
            int serverId = member.getInstanceId().toIntegerIndex();
            String expectedRole = expectedStates.get( serverId );
            boolean whatExpected = (expectedRole.equals( UNKNOWN ) || member.isAlive()) &&
                    member.getHARole().equals( expectedRole );
            if ( !whatExpected )
            {
                return false;
            }
            count++;
        }
        return count == expectedStates.size();
    }

    private static Map<Integer,String> toExpectedStatesMap( Object[] expected )
    {
        Map<Integer,String> expectedStates = new HashMap<>();
        for ( int i = 0; i < expected.length; )
        {
            expectedStates.put( (Integer) expected[i++], (String) expected[i++] );
        }
        return expectedStates;
    }

    private static HighlyAvailableGraphDatabase startDb( int serverId, File path, int clusterPort, int haPort, int initialHost1, int initialHost2 )
    {
        return (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( path )
                .setConfig( ClusterSettings.initial_hosts, String.format( "127.0.0.1:%d,127.0.0.1:%d", initialHost1, initialHost2 ) )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + clusterPort )
                .setConfig( ClusterSettings.server_id, "" + serverId )
                .setConfig( ClusterSettings.heartbeat_timeout, "5s" )
                .setConfig( ClusterSettings.default_timeout, "1s" )
                .setConfig( HaSettings.ha_server, ":" + haPort )
                .setConfig( HaSettings.tx_push_factor, "0" )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
    }

    private File path( int i )
    {
        return new File( testDirectory.graphDbDir(), "" + i );
    }
}
