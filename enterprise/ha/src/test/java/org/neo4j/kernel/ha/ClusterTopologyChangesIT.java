/**
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
package org.neo4j.kernel.ha;

import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.com.master.InvalidEpochException;
import org.neo4j.test.AbstractClusterTest;
import org.neo4j.test.ha.ClusterManager.RepairKit;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.neo4j.helpers.Predicates.not;
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

    @After
    public void cleanup()
    {
        cluster = null;
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

        kit.repair();

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
        slave1Left.await(60, SECONDS);

        // fail slave2 and await master to spot the failure
        RepairKit slave2RepairKit = cluster.fail( slave2 );
        slave2Left.await(60, SECONDS);

        // master loses quorum and goes to PENDING, cluster is unavailable
        cluster.await( not( masterAvailable() ) );
        assertEquals( HighAvailabilityMemberState.PENDING.toString(), master.getInstanceState() );

        // WHEN: both slaves are repaired, majority restored, quorum can be achieved
        slave1RepairKit.repair();
        slave2RepairKit.repair();

        // whole cluster looks fine, but slaves have stale value of the epoch if they rejoin the cluster in SLAVE state
        cluster.await( masterAvailable(  ));
        cluster.await( masterSeesSlavesAsAvailable( 2 ) );
        HighlyAvailableGraphDatabase newMaster = cluster.getMaster();

        final HighlyAvailableGraphDatabase newSlave1 = cluster.getAnySlave();
        final HighlyAvailableGraphDatabase newSlave2 = cluster.getAnySlave( newSlave1 );

        // now adding another failing listener and wait for the failure due to stale epoch
        final CountDownLatch slave1Unavailable = new CountDownLatch( 1 );
        final CountDownLatch slave2Unavailable = new CountDownLatch( 1 );
        ClusterMemberEvents clusterEvents = newMaster.getDependencyResolver().resolveDependency( ClusterMemberEvents.class );
        clusterEvents.addClusterMemberListener( new ClusterMemberListener.Adapter()
        {
            @Override
            public void memberIsUnavailable( String role, InstanceId unavailableId )
            {
                if ( instanceIdOf( newSlave1 ).equals( unavailableId ) )
                {
                    slave1Unavailable.countDown();
                }
                else if ( instanceIdOf( newSlave2 ).equals( unavailableId ) )
                {
                    slave2Unavailable.countDown();
                }
            }
        } );

        // attempt to perform transactions on both slaves throws, election is triggered
        attemptTransactions( newSlave1, newSlave2 );
        slave1Unavailable.await( 60, SECONDS ); // set a timeout in case the instance does not have stale epoch
        slave2Unavailable.await( 60, SECONDS );

        // THEN: done with election, cluster feels good and able to serve transactions
        cluster.await( allSeesAllAsAvailable() );

        assertNotNull( createNodeOn( newMaster ) );
        assertNotNull( createNodeOn( newSlave1 ) );
        assertNotNull( createNodeOn( newSlave2 ) );
    }

    @Override
    protected void configureClusterMember( GraphDatabaseBuilder builder, String clusterName, InstanceId serverId )
    {
        super.configureClusterMember( builder, clusterName, serverId );
        builder.setConfig( HaSettings.read_timeout, "1s" );
        builder.setConfig( HaSettings.state_switch_timeout, "2s" );
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

    private static void attemptTransactions( HighlyAvailableGraphDatabase... dbs )
    {
        for ( HighlyAvailableGraphDatabase db : dbs )
        {
            try
            {
                createNodeOn( db );
            }
            catch ( Exception ignored )
            {
            }
        }
    }

    private static void assertHasInvalidEpoch( HighlyAvailableGraphDatabase db )
    {
        InvalidEpochException invalidEpochException = null;
        try
        {
            createNodeOn( db );
        }
        catch ( InvalidEpochException e )
        {
            invalidEpochException = e;
        }
        assertNotNull( "Expected InvalidEpochException was not thrown", invalidEpochException );
    }
}
