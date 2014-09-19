/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.NetworkReceiver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.test.AbstractClusterTest;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.test.ReflectionUtil.getPrivateField;
import static org.neo4j.test.ha.ClusterManager.RepairKit;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;
import static org.neo4j.test.ha.ClusterManager.masterSeesSlavesAsAvailable;

public class ClusterTopologyChangesIT extends AbstractClusterTest
{
    private static final int TEST_NODE_COUNT = 10_000;

    @Before
    public void setUp()
    {
        cluster.await( allSeesAllAsAvailable() );
    }

    @Test
    public void masterRejoinsAfterFailureAndReelection() throws Throwable
    {
        // Given
        HighlyAvailableGraphDatabase initialMaster = cluster.getMaster();

        // When
        RepairKit kit = cluster.fail( initialMaster );
        cluster.await( masterAvailable( initialMaster ) );
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );

        repairUsing( kit );

        // Then
        cluster.await( masterAvailable() );
        cluster.await( allSeesAllAsAvailable() );
        assertEquals( 3, cluster.size() );
    }

    @Test
    public void slaveShouldCatchUpTransactionLogsOnFirstLockRequestAfterFailure() throws Throwable
    {
        // Given
        HighlyAvailableGraphDatabase master = cluster.getMaster();

        Node theNode;
        try ( Transaction tx = master.beginTx() )
        {
            theNode = master.createNode( label( "TheNode" ) );
            tx.success();
        }
        cluster.sync();

        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        RepairKit repairKit = cluster.fail( slave );

        try ( Transaction tx = master.beginTx() )
        {
            for ( int i = 0; i < TEST_NODE_COUNT; i++ )
            {
                master.createNode( label( "Node" + i ) );
            }
            tx.success();
        }
        cluster.sync();

        // When
        repairKit.repair();
        try ( Transaction tx = slave.beginTx() )
        {
            slave.getNodeById( theNode.getId() ).addLabel( label( "42" ) );
            tx.success();
        }

        // Then
        assertEquals( 1 + TEST_NODE_COUNT, nodeCountOn( slave ) );
    }

    @Override
    protected void configureClusterMember( GraphDatabaseBuilder builder, String clusterName, InstanceId serverId )
    {
        super.configureClusterMember( builder, clusterName, serverId );
        builder.setConfig( HaSettings.read_timeout, "1s" );
        builder.setConfig( HaSettings.state_switch_timeout, "2s" );
        builder.setConfig( HaSettings.com_chunk_size, "1024" );
    }

    @SuppressWarnings("unchecked")
    private static void repairUsing( RepairKit kit ) throws Throwable
    {
        Iterable<Lifecycle> stoppedServices = getPrivateField( kit, "stoppedServices", Iterable.class );
        for ( Lifecycle service : stoppedServices )
        {
            if ( !(service instanceof NetworkReceiver) )
            {
                service.start();
            }
        }
        Thread.sleep( 2000 );
        for ( Lifecycle service : stoppedServices )
        {
            if ( service instanceof NetworkReceiver )
            {
                service.start();
            }
        }
    }

    private static long nodeCountOn( HighlyAvailableGraphDatabase db )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            return Iterables.count( GlobalGraphOperations.at( db ).getAllNodes() );
        }
    }
}
