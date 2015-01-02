/**
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
package org.neo4j.kernel.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.com.NetworkReceiver;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.election.NotElectableElectionCredentialsProvider;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.AbstractClusterTest;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.ha.ClusterManager.RepairKit;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.neo4j.cluster.protocol.cluster.ClusterConfiguration.COORDINATOR;
import static org.neo4j.helpers.Predicates.not;
import static org.neo4j.test.ReflectionUtil.getPrivateField;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;
import static org.neo4j.test.ha.ClusterManager.masterSeesSlavesAsAvailable;

public class ClusterTopologyChangesIT extends AbstractClusterTest
{
    private static final int TEST_NODE_COUNT = 100;

    @Rule
    public final CleanupRule cleanup = new CleanupRule();

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
        assertEquals( HighAvailabilityMemberState.PENDING, master.getInstanceState() );

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

    @Test
    public void failedInstanceShouldReceiveCorrectCoordinatorIdUponRejoiningCluster() throws Throwable
    {
        // Given
        HighlyAvailableGraphDatabase initialMaster = cluster.getMaster();

        // When
        cluster.shutdown( initialMaster );
        cluster.await( masterAvailable( initialMaster ) );
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );

        // create node on new master to ensure that it has the greatest tx id
        createNodeOn( cluster.getMaster() );
        cluster.sync();

        ClusterClient clusterClient = cleanup.add( newClusterClient( new InstanceId( 1 ) ) );

        final AtomicReference<InstanceId> coordinatorIdWhenReJoined = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch( 1 );
        clusterClient.addClusterListener( new ClusterListener.Adapter()
        {
            @Override
            public void enteredCluster( ClusterConfiguration clusterConfiguration )
            {
                coordinatorIdWhenReJoined.set( clusterConfiguration.getElected( COORDINATOR ) );
                latch.countDown();
            }
        } );

        clusterClient.init();
        clusterClient.start();

        // Then
        latch.await( 2, SECONDS );
        assertEquals( new InstanceId( 2 ), coordinatorIdWhenReJoined.get() );
    }

    @Override
    protected void configureClusterMember( GraphDatabaseBuilder builder, String clusterName, InstanceId serverId )
    {
        super.configureClusterMember( builder, clusterName, serverId );
        builder.setConfig( HaSettings.read_timeout, "1s" );
        builder.setConfig( HaSettings.state_switch_timeout, "2s" );
        builder.setConfig( HaSettings.com_chunk_size, "1024" );
        builder.setConfig( GraphDatabaseSettings.cache_type, "none" );
    }

    @SuppressWarnings( "unchecked" )
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
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "key", String.valueOf( System.currentTimeMillis() ) );
            tx.success();
            return node;
        }
    }

    private ClusterClient newClusterClient( InstanceId id )
    {
        Map<String,String> configMap = MapUtil.stringMap(
                ClusterSettings.initial_hosts.name(), cluster.getInitialHostsConfigString(),
                ClusterSettings.server_id.name(), String.valueOf( id.toIntegerIndex() ),
                ClusterSettings.cluster_server.name(), "0.0.0.0:8888" );

        Config config = new Config( configMap, InternalAbstractGraphDatabase.Configuration.class,
                GraphDatabaseSettings.class );

        return new ClusterClient( new Monitors(), ClusterClient.adapt( config ), new DevNullLoggingService(),
                new NotElectableElectionCredentialsProvider(), new ObjectStreamFactory(), new ObjectStreamFactory() );
    }
}
