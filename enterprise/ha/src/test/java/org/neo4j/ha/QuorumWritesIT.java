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

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.concurrent.CountDownLatch;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

@Ignore("To be rewritten")
public class QuorumWritesIT
{
    @Test
    public void testMasterStopsWritesWhenMajorityIsUnavailable() throws Throwable
    {
        File root = TargetDirectory.forTest( getClass() ).cleanDirectory(
                "testMasterStopsWritesWhenMajorityIsUnavailable" );
        ClusterManager clusterManager = new ClusterManager( clusterOfSize( 3 ), root,
                MapUtil.stringMap( HaSettings.tx_push_factor.name(), "2", HaSettings.state_switch_timeout.name(), "5s"
                ) );
        try
        {
            clusterManager.start();
            ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();
            cluster.await( ClusterManager.masterAvailable(  ) );
            cluster.await( ClusterManager.masterSeesAllSlavesAsAvailable() );

            HighlyAvailableGraphDatabase master = cluster.getMaster();

            doTx( master );

            final CountDownLatch latch1 = new CountDownLatch( 1 );
            waitOnHeartbeatFail( master, latch1 );

            HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
            cluster.fail( slave1 );

            latch1.await();
            slave1.shutdown();

            doTx( master );

            final CountDownLatch latch2 = new CountDownLatch( 1 );
            waitOnHeartbeatFail( master, latch2 );

            HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave( slave1 );
            ClusterManager.RepairKit rk2 = cluster.fail( slave2 );

            latch2.await();

            // The master should stop saying that it's master
            assertFalse( master.isMaster() );

            try
            {
                doTx( master );
                fail( "After both slaves fail txs should not go through" );
            }
            catch ( TransactionFailureException e )
            {
                assertEquals( "Timeout waiting for cluster to elect master", e.getMessage() );
            }

            // This is not a hack, this simulates a period of inactivity in the cluster.
            Thread.sleep( 120000 ); // TODO Define "inactivity" and await that condition instead of 120 seconds.

            final CountDownLatch latch3 = new CountDownLatch( 1 );
            final CountDownLatch latch4 = new CountDownLatch( 1 );
            final CountDownLatch latch5 = new CountDownLatch( 1 );
            waitOnHeartbeatAlive( master, latch3 );
//            waitOnRoleIsAvailable( master, latch4, HighAvailabilityModeSwitcher.MASTER );
            waitOnRoleIsAvailable( master, latch5, HighAvailabilityModeSwitcher.SLAVE );

            rk2.repair();

            latch3.await();

            cluster.await( ClusterManager.masterAvailable( slave1, slave2 ) );

//            latch4.await();
            latch5.await();

            cluster.await( ClusterManager.masterAvailable(  ) );

            assertTrue( master.isMaster() );
            assertFalse( slave2.isMaster() );

            Node finalNode = doTx( master );

            try ( Transaction transaction = slave2.beginTx() )
            {
                slave2.getNodeById( finalNode.getId() );
                transaction.success();
            }
        }
        finally
        {
            clusterManager.stop();
        }
    }

    @Test
    public void testInstanceCanBeReplacedToReestablishQuorum() throws Throwable
    {
        File root = TargetDirectory.forTest( getClass() ).cleanDirectory(
                "testInstanceCanBeReplacedToReestablishQuorum"
        );
        ClusterManager clusterManager = new ClusterManager( clusterOfSize( 3 ), root,
                MapUtil.stringMap( HaSettings.tx_push_factor.name(), "2", HaSettings.state_switch_timeout.name(), "5s" ) );
        clusterManager.start();
        ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();

        HighlyAvailableGraphDatabase master = cluster.getMaster();

        cluster.await( ClusterManager.masterSeesAllSlavesAsAvailable() );

        doTx( master );

        final CountDownLatch latch1 = new CountDownLatch( 1 );
        waitOnHeartbeatFail( master, latch1 );

        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        cluster.fail( slave1 );

        latch1.await();
        slave1.shutdown();

        doTx( master );

        final CountDownLatch latch2 = new CountDownLatch( 1 );
        waitOnHeartbeatFail( master, latch2 );

        HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave( slave1 );
        cluster.fail( slave2 );

        latch2.await();

        // The master should stop saying that it's master
        assertFalse( master.isMaster() );

        try
        {
            doTx( master );
            fail( "After both slaves fail txs should not go through" );
        }
        catch ( TransactionFailureException e )
        {
            assertEquals( "Timeout waiting for cluster to elect master", e.getMessage() );
        }

        // This is not a hack, this simulates a period of inactivity in the cluster.
        Thread.sleep( 120000 ); // TODO Define "inactivity" and await that condition instead of 120 seconds.

        final CountDownLatch latch3 = new CountDownLatch( 1 );
        final CountDownLatch latch4 = new CountDownLatch( 1 );
        final CountDownLatch latch5 = new CountDownLatch( 1 );
        waitOnHeartbeatAlive( master, latch3 );
        waitOnRoleIsAvailable( master, latch4, HighAvailabilityModeSwitcher.MASTER );
        waitOnRoleIsAvailable( master, latch5, HighAvailabilityModeSwitcher.SLAVE );

        HighlyAvailableGraphDatabase replacement =
                (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( new File( root, "replacement" ).getAbsolutePath() ).
                setConfig( ClusterSettings.cluster_server, ":5010" ).
                setConfig( HaSettings.ha_server, ":6010" ).
                setConfig( ClusterSettings.server_id, "3" ).
                setConfig( ClusterSettings.initial_hosts, cluster.getInitialHostsConfigString() ).
                setConfig( HaSettings.tx_push_factor, "0" ).
                newGraphDatabase();

        latch3.await();
        latch4.await();
        latch5.await();

        assertTrue( master.isMaster() );
        assertFalse( replacement.isMaster() );

        Node finalNode = doTx( master );

        Transaction transaction = replacement.beginTx();
        try
        {
            replacement.getNodeById( finalNode.getId() );
        }
        finally
        {
            transaction.finish();
        }

        clusterManager.stop();
        replacement.shutdown();
    }

    private void waitOnHeartbeatFail( HighlyAvailableGraphDatabase master, final CountDownLatch latch )
    {
        final ClusterClient clusterClient = master.getDependencyResolver().resolveDependency( ClusterClient.class );
        clusterClient.addHeartbeatListener( new HeartbeatListener.Adapter()
        {
            @Override
            public void failed( InstanceId server )
            {
                latch.countDown();
                clusterClient.removeHeartbeatListener( this );
            }
        } );
    }

    private void waitOnHeartbeatAlive( HighlyAvailableGraphDatabase master, final CountDownLatch latch )
    {
        final ClusterClient clusterClient = master.getDependencyResolver().resolveDependency( ClusterClient.class );
        clusterClient.addHeartbeatListener( new HeartbeatListener.Adapter()
        {
            @Override
            public void alive( InstanceId server )
            {
                latch.countDown();
                clusterClient.removeHeartbeatListener( this );
            }
        } );
    }

    private void waitOnRoleIsAvailable( HighlyAvailableGraphDatabase master, final CountDownLatch latch,
                                        final String roleToWaitFor )
    {
        final ClusterMemberEvents events = master.getDependencyResolver().resolveDependency( ClusterMemberEvents
                .class );
        events.addClusterMemberListener( new ClusterMemberListener.Adapter()
        {
            @Override
            public void memberIsAvailable( String role, InstanceId availableId, URI atUri, StoreId storeId )
            {
                if ( role.equals( roleToWaitFor ) )
                {
                    latch.countDown();
                    events.removeClusterMemberListener( this );
                }
            }
        } );
    }

    private Node doTx( HighlyAvailableGraphDatabase db )
    {
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        tx.success();
        tx.finish();
        return node;
    }
}
