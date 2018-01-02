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

import java.util.Map;
import java.util.Set;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.SlaveUpdatePuller;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;

public class UpdatePullerSwitchIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() );
    private ClusterManager.ManagedCluster managedCluster;

    @Before
    public void setup() throws Exception
    {
        managedCluster = clusterRule.withProvider( ClusterManager.clusterOfSize( 2 ) )
                                    .withSharedSetting( tx_push_factor, "0" )
                                    .withSharedSetting( HaSettings.pull_interval, "100s" )
                                    .withFirstInstanceId( 6 )
                                    .startCluster();
    }

    @Test
    public void updatePullerSwitchOnNodeModeSwitch() throws Throwable
    {
        String masterLabel = "masterLabel";
        createLabeledNodeOnMaster( masterLabel );
        // force update puller to work
        pullUpdatesOnSlave();
        // node should exist on slave now
        checkLabeledNodeExistanceOnSlave( masterLabel );
        // verify that puller working on slave and not working on master
        verifyUpdatePullerThreads();

        // switch roles in cluster - now update puller should be stopped on old slave and start on old master.
        ClusterManager.RepairKit initialMasterRepairKit = managedCluster.shutdown( managedCluster.getMaster() );
        managedCluster.await( ClusterManager.masterAvailable() );

        String pretenderMasterLabel = "pretenderMasterLabel";
        createLabeledNodeOnMaster( pretenderMasterLabel );

        initialMasterRepairKit.repair();
        managedCluster.await( ClusterManager.masterSeesSlavesAsAvailable( 1 ) );

        // forcing updates pulling
        pullUpdatesOnSlave();
        checkLabeledNodeExistanceOnSlave( pretenderMasterLabel );
        // checking pulling threads
        verifyUpdatePullerThreads();


        // and finally switching roles back
        ClusterManager.RepairKit justiceRepairKit = managedCluster.shutdown( managedCluster.getMaster() );
        managedCluster.await( ClusterManager.masterAvailable() );

        String justicePrevailedLabel = "justice prevailed";
        createLabeledNodeOnMaster( justicePrevailedLabel );

        justiceRepairKit.repair();
        managedCluster.await( ClusterManager.masterSeesSlavesAsAvailable( 1 ) );

        // forcing pull updates
        pullUpdatesOnSlave();
        checkLabeledNodeExistanceOnSlave( justicePrevailedLabel );
        // checking pulling threads
        verifyUpdatePullerThreads();
    }

    private void verifyUpdatePullerThreads()
    {
        InstanceId masterId = managedCluster.getMaster().platformModule.config.get( ClusterSettings.server_id );
        InstanceId slaveId = managedCluster.getAnySlave().platformModule.config.get( ClusterSettings.server_id );
        Map<Thread,StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
        Set<Thread> threads = allStackTraces.keySet();
        assertNull( "Master should not have any puller threads", findThreadWithPrefix( threads,
                SlaveUpdatePuller.UPDATE_PULLER_THREAD_PREFIX + masterId ) );
        assertNotNull( "Slave should have active puller thread", findThreadWithPrefix( threads,
                SlaveUpdatePuller.UPDATE_PULLER_THREAD_PREFIX + slaveId ) );
    }

    /*
     * Returns the name, as a String, of first thread found that has a name starting with the provided prefix,
     * null otherwise.
     */
    private String findThreadWithPrefix( Set<Thread> threads, String prefix )
    {
        for ( Thread thread : threads )
        {
            if ( thread.getName().startsWith( prefix ) )
            {
                return thread.getName();
            }
        }
        return null;
    }

    private void pullUpdatesOnSlave() throws InterruptedException
    {
        UpdatePuller updatePuller =
                managedCluster.getAnySlave().getDependencyResolver().resolveDependency( UpdatePuller.class );
        assertTrue( "We should always have some updates to pull", updatePuller.tryPullUpdates() );
    }

    private void checkLabeledNodeExistanceOnSlave( String label )
    {
        // since we have only 2 nodes in cluster its safe to call get any cluster
        HighlyAvailableGraphDatabase slave = managedCluster.getAnySlave();
        try ( Transaction transaction = slave.beginTx() )
        {
            checkNodeWithLabelExists( slave, label );
        }

    }

    private void createLabeledNodeOnMaster( String label )
    {
        HighlyAvailableGraphDatabase master = managedCluster.getMaster();
        try ( Transaction transaction = master.beginTx() )
        {
            Node masterNode = master.createNode();
            masterNode.addLabel( DynamicLabel.label( label ) );
            transaction.success();
        }
    }

    private void checkNodeWithLabelExists( HighlyAvailableGraphDatabase database, String label  )
    {
        ResourceIterator<Node> slaveNodes = database.findNodes( DynamicLabel.label( label ) );
        assertEquals( 1, Iterables.toList( slaveNodes ).size() );
    }
}
