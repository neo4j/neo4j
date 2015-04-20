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
package org.neo4j.ha;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellLobby;
import org.neo4j.shell.ShellSettings;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;
import static org.neo4j.test.ha.ClusterManager.masterSeesSlavesAsAvailable;

public class TestPullUpdates
{
    private ClusterManager.ManagedCluster cluster;
    private static final int PULL_INTERVAL = 100;
    private static final int SHELL_PORT = 6370;
    public final @Rule TestName testName = new TestName();

    @After
    public void doAfter() throws Throwable
    {
        if ( cluster != null )
        {
            cluster.stop();
        }
    }

    @Test
    public void makeSureUpdatePullerGetsGoingAfterMasterSwitch() throws Throwable
    {
        File root = TargetDirectory.forTest( getClass() ).cleanDirectory( testName.getMethodName() );
        ClusterManager clusterManager = new ClusterManager( clusterOfSize( 3 ), root, MapUtil.stringMap(
                HaSettings.pull_interval.name(), PULL_INTERVAL+"ms",
                ClusterSettings.heartbeat_interval.name(), "2s",
                ClusterSettings.heartbeat_timeout.name(), "30s") );
        clusterManager.start();
        cluster = clusterManager.getDefaultCluster();
        cluster.await( allSeesAllAsAvailable() );

        cluster.info( "### Creating initial dataset" );
        long commonNodeId = createNodeOnMaster();

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        setProperty( master, commonNodeId, 1 );
        cluster.info( "### Initial dataset created" );
        awaitPropagation( 1, commonNodeId, cluster );

        cluster.info( "### Shutting down master" );
        ClusterManager.RepairKit masterShutdownRK = cluster.shutdown( master );

        cluster.info( "### Awaiting new master" );
        cluster.await( masterAvailable( master ) );
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );

        cluster.info( "### Doing a write to master" );
        setProperty( cluster.getMaster(), commonNodeId, 2 );
        awaitPropagation( 2, commonNodeId, cluster, master );

        cluster.info( "### Repairing cluster" );
        masterShutdownRK.repair();
        cluster.await( masterAvailable() );
        cluster.await( masterSeesSlavesAsAvailable( 2 ) );
        cluster.await( allSeesAllAsAvailable() );

        cluster.info( "### Awaiting change propagation" );
        awaitPropagation( 2, commonNodeId, cluster );
    }

    @Test
    public void pullUpdatesShellAppPullsUpdates() throws Throwable
    {
        File root = TargetDirectory.forTest( getClass() ).cleanDirectory( testName.getMethodName() );
        Map<Integer, Map<String, String>> instanceConfig = new HashMap<>();
        for (int i = 1; i <= 2; i++)
        {
            Map<String, String> thisInstance =
                    MapUtil.stringMap( ShellSettings.remote_shell_port.name(), "" + (SHELL_PORT + i) );
            instanceConfig.put( i, thisInstance );
        }
        ClusterManager clusterManager = new ClusterManager( clusterOfSize( 2 ), root, MapUtil.stringMap(
                HaSettings.pull_interval.name(), "0",
                HaSettings.tx_push_factor.name(), "0" ,
                ShellSettings.remote_shell_enabled.name(), "true"
                ), instanceConfig );
        clusterManager.start();
        cluster = clusterManager.getDefaultCluster();

        long commonNodeId = createNodeOnMaster();

        setProperty( cluster.getMaster(), commonNodeId, 1 );
        callPullUpdatesViaShell( 2 );
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        try ( Transaction tx = slave.beginTx() )
        {
            assertEquals( 1, slave.getNodeById( commonNodeId ).getProperty( "i" ) );
        }
    }

    @Test
    public void shouldPullUpdatesOnStartupNoMatterWhat() throws Exception
    {
        GraphDatabaseService slave = null;
        GraphDatabaseService master = null;
        try
        {
            File testRootDir = TargetDirectory.forTest( getClass() ).cleanDirectory( testName.getMethodName() );
            File masterDir = new File( testRootDir, "master" );
            master = new TestHighlyAvailableGraphDatabaseFactory().
                    newHighlyAvailableDatabaseBuilder( masterDir.getAbsolutePath() )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( ClusterSettings.initial_hosts, ":5001" )
                    .newGraphDatabase();

            // Copy the store, then shutdown, so update pulling later makes sense
            File slaveDir = new File( testRootDir, "slave" );
            slave = new TestHighlyAvailableGraphDatabaseFactory().
                    newHighlyAvailableDatabaseBuilder( slaveDir.getAbsolutePath() )
                    .setConfig( ClusterSettings.server_id, "2" )
                    .setConfig( ClusterSettings.initial_hosts, ":5001" )
                    .newGraphDatabase();

            // Required to block until the slave has left for sure
            final CountDownLatch slaveLeftLatch = new CountDownLatch( 1 );

            final ClusterClient masterClusterClient = ( (HighlyAvailableGraphDatabase) master ).getDependencyResolver()
                    .resolveDependency( ClusterClient.class );

            masterClusterClient.addClusterListener( new ClusterListener.Adapter()
            {
                @Override
                public void leftCluster( InstanceId instanceId, URI member )
                {
                    slaveLeftLatch.countDown();
                    masterClusterClient.removeClusterListener( this );
                }
            } );

            ((GraphDatabaseAPI)master).getDependencyResolver().resolveDependency( StringLogger.class ).info( "SHUTTING DOWN SLAVE" );
            slave.shutdown();

            // Make sure that the slave has left, because shutdown() may return before the master knows
            if (!slaveLeftLatch.await(60, TimeUnit.SECONDS))
            {
                throw new IllegalStateException( "Timeout waiting for slave to leave" );
            }

            long nodeId;
            try ( Transaction tx = master.beginTx() )
            {
                Node node = master.createNode();
                node.setProperty( "from", "master" );
                nodeId = node.getId();
                tx.success();
            }

            // Store is already in place, should pull updates
            slave = new TestHighlyAvailableGraphDatabaseFactory().
                    newHighlyAvailableDatabaseBuilder( slaveDir.getAbsolutePath() )
                    .setConfig( ClusterSettings.server_id, "2" )
                    .setConfig( ClusterSettings.initial_hosts, ":5001" )
                    .setConfig( HaSettings.pull_interval, "0" ) // no pull updates, should pull on startup
                    .newGraphDatabase();

            slave.beginTx().close(); // Make sure switch to slave completes and so does the update pulling on startup

            try ( Transaction tx = slave.beginTx() )
            {
                assertEquals( "master", slave.getNodeById( nodeId ).getProperty( "from" ) );
                tx.success();
            }
        }
        finally
        {
            if ( slave != null)
            {
                slave.shutdown();
            }
            if ( master != null )
            {
                master.shutdown();
            }
        }
    }

    private long createNodeOnMaster()
    {
        try ( Transaction tx = cluster.getMaster().beginTx() )
        {
            long id = cluster.getMaster().createNode().getId();
            tx.success();
            return id;
        }
    }

    private void callPullUpdatesViaShell( int i ) throws ShellException
    {
        ShellClient client = ShellLobby.newClient( SHELL_PORT + i );
        client.evaluate( "pullupdates" );
    }

    private void powerNap() throws InterruptedException
    {
        Thread.sleep( 50 );
    }

    private void awaitPropagation( int expectedPropertyValue, long nodeId, ClusterManager.ManagedCluster cluster, HighlyAvailableGraphDatabase... excepts ) throws Exception
    {
        long endTime = currentTimeMillis() + PULL_INTERVAL * 20;
        boolean ok = false;
        while ( !ok && currentTimeMillis() < endTime )
        {
            ok = true;
            loop:
            for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
            {
                for (HighlyAvailableGraphDatabase except: excepts) {
                    if (db == except){
                        continue loop;
                    }
                }
                try ( Transaction tx = db.beginTx() )
                {
                    Number value = (Number)db.getNodeById(nodeId).getProperty( "i", null );
                    if ( value == null || value.intValue() != expectedPropertyValue )
                    {
                        ok = false;
                    }
                }
                catch( NotFoundException e )
                {
                    ok = false;
                }
            }
            if ( !ok )
            {
                powerNap();
            }
        }
        assertTrue( "Change wasn't propagated by pulling updates", ok );
    }

    private void setProperty( HighlyAvailableGraphDatabase db, long nodeId, int i ) throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.getNodeById( nodeId ).setProperty( "i", i );
            tx.success();
        }
    }
}
