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
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.member.paxos.MemberIsAvailable;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HardKillIT
{
    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    private ProcessStreamHandler processHandler;

    @Test
    public void testMasterSwitchHappensOnKillMinus9() throws Exception
    {
        Process proc = null;
        HighlyAvailableGraphDatabase dbWithId2 = null, dbWithId3 = null, oldMaster = null;
        try
        {
            proc = run( 1 );
            Thread.sleep( 12000 );
            dbWithId2 = startDb( 2, path( 2 ) );
            dbWithId3 = startDb( 3, path( 3 ) );

            assertTrue( !dbWithId2.isMaster() );
            assertTrue( !dbWithId3.isMaster() );

            final CountDownLatch newMasterAvailableLatch = new CountDownLatch( 1 );
            dbWithId2.getDependencyResolver().resolveDependency( ClusterClient.class ).addAtomicBroadcastListener(
                    new AtomicBroadcastListener()
                    {
                        @Override
                        public void receive( Payload value )
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
                        }
                    } );
            proc.destroy();
            proc = null;

            assertTrue( newMasterAvailableLatch.await( 60, SECONDS ) );

            assertTrue( dbWithId2.isMaster() );
            assertTrue( !dbWithId3.isMaster() );

            // Ensure that everyone has marked the killed instance as failed, otherwise it cannot rejoin
            Thread.sleep( 15000 );

            oldMaster = startDb( 1, path( 1 ) );
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
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
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

    private Process run( int machineId ) throws IOException
    {
        List<String> allArgs = new ArrayList<String>( Arrays.asList( "java", "-cp",
                System.getProperty( "java.class.path" ), "-Djava.awt.headless=true", HardKillIT.class.getName() ) );
        allArgs.add( "" + machineId );
        allArgs.add( path( machineId ).getAbsolutePath() );

        Process process = Runtime.getRuntime().exec( allArgs.toArray( new String[allArgs.size()] ) );
        processHandler = new ProcessStreamHandler( process, false );
        processHandler.launch();
        return process;
    }

    /*
     * Used to launch the master instance
     */
    public static void main( String[] args )
    {
        startDb( Integer.parseInt( args[0] ), new File( args[1] ) );
    }

    private static HighlyAvailableGraphDatabase startDb( int serverId, File path )
    {
        GraphDatabaseBuilder builder = new TestHighlyAvailableGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( path )
                .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:7102,127.0.0.1:7103" )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + (7101 + serverId) )
                .setConfig( ClusterSettings.server_id, "" + serverId )
                .setConfig( HaSettings.ha_server, ":" + (7501 + serverId) )
                .setConfig( HaSettings.tx_push_factor, "0" );
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) builder.newGraphDatabase();
        db.beginTx().close();
        try
        {
            Thread.sleep( 2000 );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        return db;
    }

    private File path( int i )
    {
        return new File( testDirectory.graphDbDir(), "" + i );
    }
}
