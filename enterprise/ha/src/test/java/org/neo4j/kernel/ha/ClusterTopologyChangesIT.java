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

import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.com.NetworkReceiver;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.test.AbstractClusterTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.neo4j.helpers.Predicates.not;
import static org.neo4j.test.ReflectionUtil.getPrivateField;
import static org.neo4j.test.ha.ClusterManager.RepairKit;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;
import static org.neo4j.test.ha.ClusterManager.masterSeesSlavesAsAvailable;

public class ClusterTopologyChangesIT extends AbstractClusterTest
{
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
    public void slaveShouldServeTxsAfterMasterLostQuorumWentToPendingAndThenQuorumWasRestored() throws Throwable
    {
        // GIVEN: cluster with 3 members
        HighlyAvailableGraphDatabase master = cluster.getMaster();

        final HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        final HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave( slave1 );

        final CountDownLatch slave1Left = new CountDownLatch( 1 );
        final CountDownLatch slave2Left = new CountDownLatch( 1 );

        clusterClientOf( master ).addHeartbeatListener( new HeartbeatListener.Adapter()
        {
            @Override
            public void failed( InstanceId server )
            {
                if ( instanceIdOf( slave1 ).equals( server ) )
                {
                    slave1Left.countDown();
                }
                else if ( instanceIdOf( slave2 ).equals( server ) )
                {
                    slave2Left.countDown();
                }
            }
        } );

        // fail slave1 and await master to spot the failure
        RepairKit slave1RepairKit = cluster.fail( slave1 );
        slave1Left.await();

        // fail slave2 and await master to spot the failure
        RepairKit slave2RepairKit = cluster.fail( slave2 );
        slave2Left.await();

        // master loses quorum and goes to PENDING, cluster is unavailable
        cluster.await( not( masterAvailable() ) );
        assertEquals( HighAvailabilityMemberState.PENDING.toString(), master.getInstanceState() );

        // WHEN: both slaves are repaired, majority restored, quorum can be achieved
        slave1RepairKit.repair();
        slave2RepairKit.repair();

        // THEN: whole cluster should be fine, every member should be available
        cluster.await( masterAvailable() );
        cluster.await( masterSeesSlavesAsAvailable( 2 ) );
        cluster.await( allSeesAllAsAvailable() );

        // able to perform transactions on master and slaves
        assertNotNull( createNodeOn( master ) );
        assertNotNull( createNodeOn( slave1 ) );
        assertNotNull( createNodeOn( slave2 ) );
    }

    @Override
    protected void configureClusterMember( GraphDatabaseBuilder builder, String clusterName, InstanceId serverId )
    {
        super.configureClusterMember( builder, clusterName, serverId );
        builder.setConfig( HaSettings.read_timeout, "1s" );
        builder.setConfig( HaSettings.state_switch_timeout, "2s" );
    }

    @SuppressWarnings("unchecked")
    private void repairUsing( RepairKit kit ) throws Throwable
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

    private static ClusterClient clusterClientOf( HighlyAvailableGraphDatabase db )
    {
        return db.getDependencyResolver().resolveDependency( ClusterClient.class );
    }

    private static InstanceId instanceIdOf( HighlyAvailableGraphDatabase db )
    {
        return clusterClientOf( db ).getServerId();
    }

    private static Node createNodeOn( HighlyAvailableGraphDatabase db )
    {
        Node node;
        Transaction tx = db.beginTx();
        try
        {
            node = db.createNode();
            node.setProperty( "key", String.valueOf( System.currentTimeMillis() ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        return node;
    }
}
