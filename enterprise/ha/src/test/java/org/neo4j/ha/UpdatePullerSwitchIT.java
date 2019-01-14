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

import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;
import static org.neo4j.kernel.ha.SlaveUpdatePuller.UPDATE_PULLER_THREAD_PREFIX;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;

public class UpdatePullerSwitchIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule().withCluster( clusterOfSize( 2 ) )
            .withSharedSetting( tx_push_factor, "0" )
            .withSharedSetting( HaSettings.pull_interval, "100s" )
            .withFirstInstanceId( 6 );

    @Test
    public void updatePullerSwitchOnNodeModeSwitch() throws Throwable
    {
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();

        Label firstLabel = Label.label( "firstLabel" );
        createLabeledNodeOnMaster( cluster, firstLabel );
        // force update puller to work
        pullUpdatesOnSlave( cluster );
        // node should exist on slave now
        checkLabeledNodeExistanceOnSlave( cluster, firstLabel );
        // verify that puller working on slave and not working on master
        verifyUpdatePullerThreads( cluster );

        for ( int i = 1; i <= 2; i++ )
        {
            // switch roles in cluster - now update puller should be stopped on old slave and start on old master.
            ClusterManager.RepairKit repairKit = cluster.shutdown( cluster.getMaster() );
            cluster.await( masterAvailable() );

            Label currentLabel = Label.label( "label_" + i );

            createLabeledNodeOnMaster( cluster, currentLabel );

            repairKit.repair();
            cluster.await( allSeesAllAsAvailable(), 120 );

            // forcing updates pulling
            pullUpdatesOnSlave( cluster );
            checkLabeledNodeExistanceOnSlave( cluster, currentLabel );
            // checking pulling threads
            verifyUpdatePullerThreads( cluster );
        }
    }

    private void verifyUpdatePullerThreads( ClusterManager.ManagedCluster cluster )
    {
        Map<Thread,StackTraceElement[]> threads = Thread.getAllStackTraces();
        Optional<Map.Entry<Thread,StackTraceElement[]>> masterEntry =
                findThreadWithPrefix( threads, UPDATE_PULLER_THREAD_PREFIX + serverId( cluster.getMaster() ) );
        assertFalse( format( "Found an update puller on master.%s", masterEntry.map( this::prettyPrint ).orElse( "" ) ),
                masterEntry.isPresent() );

        Optional<Map.Entry<Thread,StackTraceElement[]>> slaveEntry =
                findThreadWithPrefix( threads, UPDATE_PULLER_THREAD_PREFIX + serverId( cluster.getAnySlave() ) );
        assertTrue( "Found no update puller on slave", slaveEntry.isPresent() );
    }

    private String prettyPrint( Map.Entry<Thread,StackTraceElement[]> entry )
    {
        return format( "\n\tThread: %s\n\tStackTrace: %s", entry.getKey(), Arrays.toString( entry.getValue() ) );
    }

    private InstanceId serverId( HighlyAvailableGraphDatabase db )
    {
        return db.getDependencyResolver().resolveDependency( Config.class ).get( ClusterSettings.server_id );
    }

    private Optional<Map.Entry<Thread,StackTraceElement[]>> findThreadWithPrefix(
            Map<Thread,StackTraceElement[]> threads, String prefix )
    {
        return threads.entrySet().stream()
                .filter( entry -> entry.getKey().getName().startsWith( prefix ) ).findFirst();
    }

    private void pullUpdatesOnSlave( ClusterManager.ManagedCluster cluster ) throws InterruptedException
    {
        UpdatePuller updatePuller =
                cluster.getAnySlave().getDependencyResolver().resolveDependency( UpdatePuller.class );
        assertTrue( "We should always have some updates to pull", updatePuller.tryPullUpdates() );
    }

    private void checkLabeledNodeExistanceOnSlave( ClusterManager.ManagedCluster cluster, Label label )
    {
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        try ( Transaction transaction = slave.beginTx() )
        {
            ResourceIterator<Node> slaveNodes = slave.findNodes( label );
            assertEquals( 1, Iterators.asList( slaveNodes ).size() );
            transaction.success();
        }
    }

    private void createLabeledNodeOnMaster( ClusterManager.ManagedCluster cluster, Label label )
    {
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        try ( Transaction transaction = master.beginTx() )
        {
            Node masterNode = master.createNode();
            masterNode.addLabel( label );
            transaction.success();
        }
    }
}
