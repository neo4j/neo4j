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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleListener;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.Exceptions.contains;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;

public class ClusterTransactionIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() );

    private ClusterManager.ManagedCluster cluster;

    @Before
    public void setUp() throws Exception
    {
        cluster = clusterRule.withProvider( clusterOfSize( 3 ) )
                             .withSharedSetting( HaSettings.ha_server, ":6001-6005" )
                             .withSharedSetting( HaSettings.tx_push_factor, "2" ).startCluster();

        cluster.await( ClusterManager.allSeesAllAsAvailable() );
    }

    @Test
    public void givenClusterWhenShutdownMasterThenCannotStartTransactionOnSlave() throws Throwable
    {
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
}
