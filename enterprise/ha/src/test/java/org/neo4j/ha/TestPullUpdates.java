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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.function.IntFunction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellLobby;
import org.neo4j.shell.ShellSettings;
import org.neo4j.test.ha.ClusterRule;

import static java.lang.System.currentTimeMillis;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterSeesSlavesAsAvailable;

public class TestPullUpdates
{
    private static final int PULL_INTERVAL = 100;
    private static final int SHELL_PORT = 6370;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() );

    @Test
    public void makeSureUpdatePullerGetsGoingAfterMasterSwitch() throws Throwable
    {
        ClusterManager.ManagedCluster cluster = clusterRule.
                withSharedSetting( HaSettings.pull_interval, PULL_INTERVAL + "ms" ).
                withSharedSetting( ClusterSettings.heartbeat_interval, "2s" ).
                withSharedSetting( ClusterSettings.heartbeat_timeout, "30s" ).
                startCluster();

        cluster.info( "### Creating initial dataset" );
        long commonNodeId = createNodeOnMaster( cluster );

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
    public void terminatedTransactionDoesNotForceUpdatePullingWithTxTerminationAwareLocks() throws Throwable
    {
        int testTxsOnMaster = 42;

        ClusterManager.ManagedCluster cluster = clusterRule.
                withSharedSetting( HaSettings.pull_interval, "0s" ).
                withSharedSetting( HaSettings.tx_push_factor, "0" ).
                withSharedSetting( KernelTransactions.tx_termination_aware_locks, Settings.TRUE ).
                startCluster();

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        final HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        createNodeOn( master );
        cluster.sync();

        long lastClosedTxIdOnMaster = lastClosedTxIdOn( master );
        long lastClosedTxIdOnSlave = lastClosedTxIdOn( slave );

        final CountDownLatch slaveTxStarted = new CountDownLatch( 1 );
        final CountDownLatch slaveShouldCommit = new CountDownLatch( 1 );
        final AtomicReference<Transaction> slaveTx = new AtomicReference<>();
        Future<?> slaveCommit = Executors.newSingleThreadExecutor().submit( new Runnable()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = slave.beginTx() )
                {
                    slaveTx.set( tx );
                    slaveTxStarted.countDown();
                    await( slaveShouldCommit );
                    tx.success();
                }
            }
        } );

        await( slaveTxStarted );
        createNodesOn( master, testTxsOnMaster );

        assertNotNull( slaveTx.get() );
        slaveTx.get().terminate();
        slaveShouldCommit.countDown();

        try
        {
            slaveCommit.get();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ExecutionException.class ) );
            assertThat( e.getCause(), instanceOf( TransientTransactionFailureException.class ) );
        }

        assertEquals( lastClosedTxIdOnMaster + testTxsOnMaster, lastClosedTxIdOn( master ) );
        assertEquals( lastClosedTxIdOnSlave, lastClosedTxIdOn( slave ) );
    }

    @Test
    public void pullUpdatesShellAppPullsUpdates() throws Throwable
    {
        ClusterManager.ManagedCluster cluster = clusterRule.withProvider( clusterOfSize( 2 ) ).
                withSharedSetting( HaSettings.pull_interval, "0" ).
                withSharedSetting( HaSettings.tx_push_factor, "0" ).
                withSharedSetting( ShellSettings.remote_shell_enabled, Settings.TRUE ).
                withInstanceSetting( ShellSettings.remote_shell_port, new IntFunction<String>()
                {
                    @Override
                    public String apply( int oneBasedServerId )
                    {
                        return oneBasedServerId >= 1 && oneBasedServerId <= 2 ?
                               "" + (SHELL_PORT + oneBasedServerId) : null;
                    }
                } ).
                startCluster();

        long commonNodeId = createNodeOnMaster( cluster );

        setProperty( cluster.getMaster(), commonNodeId, 1 );
        callPullUpdatesViaShell( 2 );
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        try ( Transaction tx = slave.beginTx() )
        {
            assertEquals( 1, slave.getNodeById( commonNodeId ).getProperty( "i" ) );
            tx.success();
        }
    }

    @Test
    public void shouldPullUpdatesOnStartupNoMatterWhat() throws Exception
    {
        HighlyAvailableGraphDatabase slave = null;
        HighlyAvailableGraphDatabase master = null;
        try
        {
            File testRootDir = clusterRule.cleanDirectory( "shouldPullUpdatesOnStartupNoMatterWhat" );
            File masterDir = new File( testRootDir, "master" );
            master = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                    newEmbeddedDatabaseBuilder( masterDir )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( ClusterSettings.initial_hosts, "localhost:5001" )
                    .newGraphDatabase();

            // Copy the store, then shutdown, so update pulling later makes sense
            File slaveDir = new File( testRootDir, "slave" );
            slave =  (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                    newEmbeddedDatabaseBuilder( slaveDir )
                    .setConfig( ClusterSettings.server_id, "2" )
                    .setConfig( ClusterSettings.initial_hosts, "localhost:5001" )
                    .newGraphDatabase();

            // Required to block until the slave has left for sure
            final CountDownLatch slaveLeftLatch = new CountDownLatch( 1 );
            final ClusterClient masterClusterClient =
                    master.getDependencyResolver().resolveDependency( ClusterClient.class );
            masterClusterClient.addClusterListener( new ClusterListener.Adapter()
            {
                @Override
                public void leftCluster( InstanceId instanceId, URI member )
                {
                    slaveLeftLatch.countDown();
                    masterClusterClient.removeClusterListener( this );
                }
            } );

            master.getDependencyResolver().resolveDependency( LogService.class )
                    .getInternalLog( getClass() ).info( "SHUTTING DOWN SLAVE" );
            slave.shutdown();
            slave = null;

            // Make sure that the slave has left, because shutdown() may return before the master knows
            assertTrue( "Timeout waiting for slave to leave", slaveLeftLatch.await( 60, TimeUnit.SECONDS ) );

            long nodeId;
            try ( Transaction tx = master.beginTx() )
            {
                Node node = master.createNode();
                node.setProperty( "from", "master" );
                nodeId = node.getId();
                tx.success();
            }

            // Store is already in place, should pull updates
            slave = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                    newEmbeddedDatabaseBuilder( slaveDir )
                    .setConfig( ClusterSettings.server_id, "2" )
                    .setConfig( ClusterSettings.initial_hosts, "localhost:5001" )
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

    private long createNodeOnMaster( ClusterManager.ManagedCluster cluster )
    {
        return createNodeOn( cluster.getMaster() );
    }

    private static void createNodesOn( GraphDatabaseService db, int count )
    {
        for ( int i = 0; i < count; i++ )
        {
            createNodeOn( db );
        }
    }

    private static long createNodeOn( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            long id = db.createNode().getId();
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

    private void await( CountDownLatch latch )
    {
        try
        {
            assertTrue( latch.await( 1, TimeUnit.MINUTES ) );
        }
        catch ( InterruptedException e )
        {
            throw new AssertionError( e );
        }
    }

    private long lastClosedTxIdOn( GraphDatabaseAPI db )
    {
        TransactionIdStore txIdStore = db.getDependencyResolver().resolveDependency( TransactionIdStore.class );
        return txIdStore.getLastClosedTransactionId();
    }
}
