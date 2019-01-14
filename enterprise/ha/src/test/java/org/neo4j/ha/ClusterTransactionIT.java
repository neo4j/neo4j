/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.FutureTask;

import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleStatus;
import org.neo4j.test.ha.ClusterRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.Exceptions.contains;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;

public class ClusterTransactionIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule();

    private ClusterManager.ManagedCluster cluster;

    @Before
    public void setUp()
    {
        cluster = clusterRule.withCluster( clusterOfSize( 3 ) )
                             .withSharedSetting( HaSettings.tx_push_factor, "2" )
                             .withSharedSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                             .startCluster();

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
        final FutureTask<Boolean> result = new FutureTask<>( () ->
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
        } );

        master.getDependencyResolver()
                .resolveDependency( LifeSupport.class )
                .addLifecycleListener( ( instance, from, to ) ->
                {
                    if ( instance.getClass().getName().contains( "DatabaseAvailability" ) &&
                         to == LifecycleStatus.STOPPED )
                    {
                        result.run();
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
            assertThat( Iterables.count( master.getAllNodes() ), is( 3L ) );
        }
    }
}
