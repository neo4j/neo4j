/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.Exceptions.contains;
import static org.neo4j.helpers.NamedThreadFactory.named;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;

public class ClusterTransactionIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withProvider( clusterOfSize( 3 ) )
            .withSharedSetting( HaSettings.ha_server, ":6001-6005" )
            .withSharedSetting( HaSettings.tx_push_factor, "2" );

    @Test
    public void givenClusterWhenShutdownMasterThenCannotStartTransactionOnSlave() throws Throwable
    {
        ClusterManager.ManagedCluster cluster = startCluster();

        final HighlyAvailableGraphDatabase master = cluster.getMaster();
        final HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        final long nodeId;
        try ( Transaction tx = master.beginTx() )
        {
            nodeId = master.createNode().getId();
            tx.success();
        }

        cluster.sync();

        // When
        final FutureTask<Boolean> result = new FutureTask<>( new Callable<Boolean>()
        {
            @Override
            public Boolean call() throws Exception
            {
                try ( Transaction tx = slave.beginTx() )
                {
                    tx.acquireWriteLock( slave.getNodeById( nodeId ) );
                }
                catch ( Exception e )
                {
                    return contains( e, TransactionFailureException.class );
                }
                // Fail otherwise
                return false;
            }
        } );

        master.getDependencyResolver()
                .resolveDependency( LifeSupport.class )
                .addLifecycleListener( new LifecycleListener()
                {
                    @Override
                    public void notifyStatusChanged( Object instance, LifecycleStatus from, LifecycleStatus to )
                    {
                        if ( instance.getClass().getName().contains( "DatabaseAvailability" ) &&
                             to == LifecycleStatus.STOPPED )
                        {
                            result.run();
                        }
                    }
                } );

        master.shutdown();

        // Then
        assertThat( result.get(), equalTo( true ) );
    }

    @Test
    public void slaveMustConnectLockManagerToNewMasterAfterTwoOtherClusterMembersRoleSwitch() throws Throwable
    {
        ClusterManager.ManagedCluster cluster = startCluster();

        final HighlyAvailableGraphDatabase initialMaster = cluster.getMaster();
        HighlyAvailableGraphDatabase firstSlave = cluster.getAnySlave();
        HighlyAvailableGraphDatabase secondSlave = cluster.getAnySlave( firstSlave );

        // Run a transaction on the slaves, to make sure that a master connection has been initialised in all
        // internal pools.
        try ( Transaction tx = firstSlave.beginTx() )
        {
            firstSlave.createNode();
            tx.success();
        }
        try ( Transaction tx = secondSlave.beginTx() )
        {
            secondSlave.createNode();
            tx.success();
        }
        cluster.sync();

        ClusterManager.RepairKit failedMaster = cluster.fail( initialMaster );
        cluster.await( ClusterManager.masterAvailable( initialMaster ) );
        failedMaster.repair();
        cluster.await( ClusterManager.masterAvailable( initialMaster ) );
        cluster.await( ClusterManager.allSeesAllAsAvailable() );

        // The cluster has now switched the master role to one of the slaves.
        // The slave that didn't switch, should still have done the work to reestablish the connection to the new
        // master.
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave( initialMaster );
        try ( Transaction tx = slave.beginTx() )
        {
            slave.createNode();
            tx.success();
        }

        // We assert that the transaction above does not throw any exceptions, and that we have now created 3 nodes.
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            GlobalGraphOperations gops = GlobalGraphOperations.at( master );
            assertThat( IteratorUtil.count( gops.getAllNodes() ), is( 3 ) );
        }
    }

    @Test
    public void terminateSlaveTransactionThatWaitsForLockOnMaster() throws Exception
    {
        clusterRule.withSharedSetting( HaSettings.lock_read_timeout, "1m" );
        clusterRule.withSharedSetting( KernelTransactions.tx_termination_aware_locks, Settings.TRUE );

        ClusterManager.ManagedCluster cluster = startCluster();

        final Label label = DynamicLabel.label( "foo" );
        final String property = "bar";
        final String masterValue = "master";
        final String slaveValue = "slave";

        final HighlyAvailableGraphDatabase master = cluster.getMaster();
        final HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        createNodeWithLabel( cluster, label );

        final CountDownLatch masterTxCommit = new CountDownLatch( 1 );
        Future<?> masterTx = newSingleThreadExecutor( named( "masterTx" ) ).submit( new Runnable()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = master.beginTx() )
                {
                    Node node = single( master.findNodes( label ) );
                    node.setProperty( property, masterValue );
                    await( masterTxCommit );
                    tx.success();
                }
            }
        } );

        final AtomicReference<Transaction> slaveTxReference = new AtomicReference<>();
        final CountDownLatch slaveTxStarted = new CountDownLatch( 1 );
        Future<?> slaveTx = newSingleThreadExecutor( named( "slaveTx" ) ).submit( new Runnable()
        {
            @Override
            public void run()
            {
                try ( Transaction tx = slave.beginTx() )
                {
                    slaveTxReference.set( tx );
                    Node node = single( slave.findNodes( label ) );
                    slaveTxStarted.countDown();
                    node.setProperty( property, slaveValue );
                    tx.success();
                }
            }
        } );

        slaveTxStarted.await();
        Thread.sleep( 2000 );

        terminate( slaveTxReference );
        assertTxWasTerminated( slaveTx );

        masterTxCommit.countDown();
        assertNull( masterTx.get() );
        assertSingleNodeExists( master, label, property, masterValue );
    }

    private void createNodeWithLabel( ClusterManager.ManagedCluster cluster, Label label ) throws InterruptedException
    {
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction tx = master.beginTx() )
        {
            master.createNode( label );
            tx.success();
        }

        cluster.sync();
    }

    private void assertSingleNodeExists( HighlyAvailableGraphDatabase db, Label label, String property, String value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = single( db.findNodes( label ) );
            assertTrue( node.hasProperty( property ) );
            assertEquals( value, node.getProperty( property ) );
            tx.success();
        }
    }

    private void terminate( AtomicReference<Transaction> txReference )
    {
        Transaction tx = txReference.get();
        assertNotNull( tx );
        tx.terminate();
    }

    private void assertTxWasTerminated( Future<?> txFuture ) throws InterruptedException
    {
        try
        {
            txFuture.get();
            fail( "Exception expected" );
        }
        catch ( ExecutionException e )
        {
            e.printStackTrace();
            assertThat( e.getCause(), instanceOf( TransactionTerminatedException.class ) );
        }
    }

    private static void await( CountDownLatch latch )
    {
        try
        {
            assertTrue( latch.await( 2, TimeUnit.MINUTES ) );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    private ClusterManager.ManagedCluster startCluster() throws Exception
    {
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        cluster.await( ClusterManager.allSeesAllAsAvailable() );
        return cluster;
    }
}
