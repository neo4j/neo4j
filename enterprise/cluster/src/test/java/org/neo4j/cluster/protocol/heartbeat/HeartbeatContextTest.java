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
package org.neo4j.cluster.protocol.heartbeat;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context.MultiPaxosContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.ElectionRole;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests basic sanity and various scenarios for HeartbeatContext.
 * All tests are performed from the perspective of instance running at 5001.
 */
public class HeartbeatContextTest
{
    private static InstanceId[] instanceIds = new InstanceId[]{
            new InstanceId( 1 ),
            new InstanceId( 2 ),
            new InstanceId( 3 )
    };

    private static String[] initialHosts = new String[]{
            "cluster://localhost:5001",
            "cluster://localhost:5002",
            "cluster://localhost:5003"
    };

    private HeartbeatContext toTest;
    private ClusterContext context;

    @Before
    public void setup()
    {
        Map<InstanceId, URI> members = new HashMap<>();
        for ( int i = 0; i < instanceIds.length; i++ )
        {
            members.put( instanceIds[i], URI.create( initialHosts[i] ) );
        }
        ClusterConfiguration config = new ClusterConfiguration( "clusterName", NullLogProvider.getInstance(), initialHosts );
        config.setMembers( members );

        context = mock( ClusterContext.class );

        Config configuration = mock( Config.class );
        when( configuration.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        when( context.getConfiguration() ).thenReturn( config );
        when( context.getMyId() ).thenReturn( instanceIds[0] );

        MultiPaxosContext context = new MultiPaxosContext( instanceIds[0], Iterables.iterable(
                        new ElectionRole( "coordinator" ) ), config,
                        Mockito.mock( Executor.class ), NullLogProvider.getInstance(),
                        Mockito.mock( ObjectInputStreamFactory.class), Mockito.mock( ObjectOutputStreamFactory.class),
                        Mockito.mock( AcceptorInstanceStore.class), Mockito.mock( Timeouts.class),
                        mock( ElectionCredentialsProvider.class ), configuration );

        toTest = context.getHeartbeatContext();
    }

    @Test
    public void testSaneInitialState()
    {

        // In config, not suspected yet
        assertFalse( toTest.alive( instanceIds[0] ) );
        // Not in config
        assertFalse( toTest.alive( new InstanceId( 4 ) ) );

        // By default, instances start off as alive
        assertEquals( instanceIds.length, Iterables.count( toTest.getAlive() ) );
        assertEquals( 0, toTest.getFailed().size() );

        for ( InstanceId initialHost : instanceIds )
        {
            assertFalse( toTest.isFailedBasedOnSuspicions( initialHost ) );
        }
    }

    @Test
    public void testSuspicions()
    {
        InstanceId suspect = instanceIds[1];
        toTest.suspect( suspect );
        assertEquals( Collections.singleton( suspect ), toTest.getSuspicionsFor( context.getMyId() ) );
        assertEquals( Collections.singletonList( context.getMyId() ), toTest.getSuspicionsOf( suspect ) );
        // Being suspected by just one (us) is not enough
        assertFalse( toTest.isFailedBasedOnSuspicions( suspect ) );
        assertTrue( toTest.alive( suspect ) ); // This resets the suspicion above

        // If we suspect an instance twice in a row, it shouldn't change its status in any way.
        toTest.suspect( suspect );
        toTest.suspect( suspect );
        assertEquals( Collections.singleton( suspect ), toTest.getSuspicionsFor( context.getMyId() ) );
        assertEquals( Collections.singletonList( context.getMyId() ), toTest.getSuspicionsOf( suspect ) );
        assertFalse( toTest.isFailedBasedOnSuspicions( suspect ) );
        assertTrue( toTest.alive( suspect ) );

        // The other one sends suspicions too
        InstanceId newSuspiciousBastard = instanceIds[2];
        toTest.suspicions( newSuspiciousBastard, Collections.singleton( suspect ) );
        toTest.suspect( suspect );
        // Now two instances suspect it, it should be reported failed
        assertEquals( Collections.singleton( suspect ), toTest.getSuspicionsFor( context.getMyId() ) );
        assertEquals( Collections.singleton( suspect ), toTest.getSuspicionsFor( newSuspiciousBastard ) );
        List<InstanceId> suspiciousBastards = new ArrayList<>( 2 );
        suspiciousBastards.add( context.getMyId() );
        suspiciousBastards.add( newSuspiciousBastard );
        assertEquals( suspiciousBastards, toTest.getSuspicionsOf( suspect ) );
        assertTrue( toTest.isFailedBasedOnSuspicions( suspect ) );
        assertTrue( toTest.alive( suspect ) );
    }

    @Test
    public void testFailedInstanceReportingSuspicions()
    {
        InstanceId suspect = instanceIds[1];
        InstanceId newSuspiciousBastard = instanceIds[2];
        toTest.suspicions( newSuspiciousBastard, Collections.singleton( suspect ) );
        toTest.suspect( suspect );

        // Just make sure
        assertTrue( toTest.isFailedBasedOnSuspicions( suspect ) );

        // Suspicions of a failed instance should be ignored
        toTest.suspicions( suspect, Collections.singleton( newSuspiciousBastard ) );
        assertTrue( "Suspicions should have been ignored", toTest.getSuspicionsOf( newSuspiciousBastard ).isEmpty() );
    }

    @Test
    public void testFailedInstanceBecomingAlive()
    {
        InstanceId suspect = instanceIds[1];
        InstanceId newSuspiciousBastard = instanceIds[2];
        toTest.suspicions( newSuspiciousBastard, Collections.singleton( suspect ) );
        toTest.suspect( suspect );

        // Just make sure
        assertTrue( toTest.isFailedBasedOnSuspicions( suspect ) );

        // Ok, here it is. We received a heartbeat, so it is alive.
        toTest.alive( suspect );
        // It must no longer be failed
        assertFalse( toTest.isFailedBasedOnSuspicions( suspect ) );

        // Simulate us stopping receiving heartbeats again
        toTest.suspect( suspect );
        assertTrue( toTest.isFailedBasedOnSuspicions( suspect ) );

        // Assume the other guy started receiving heartbeats first
        toTest.suspicions( newSuspiciousBastard, Collections.emptySet() );
        assertFalse( toTest.isFailedBasedOnSuspicions( suspect ) );
    }

    /**
     * Tests the following scenario:
     * Instance A (the one this test simulates) sees instance C down. B agrees.
     * Instance A sees instance B down.
     * Instance C starts responding again.
     * Instance A should now consider C alive.
     */
    @Test
    public void testOneInstanceComesAliveAfterAllOtherFail()
    {
        InstanceId instanceB = instanceIds[1];
        InstanceId instanceC = instanceIds[2];

        // Both A and B consider C down
        toTest.suspect( instanceC );
        toTest.suspicions( instanceB, Collections.singleton( instanceC ) );
        assertTrue( toTest.isFailedBasedOnSuspicions( instanceC ) );

        // A sees B as down
        toTest.suspect( instanceB );
        assertTrue( toTest.isFailedBasedOnSuspicions( instanceB ) );

        // C starts responding again
        assertTrue( toTest.alive( instanceC ) );

        assertFalse( toTest.isFailedBasedOnSuspicions( instanceC ) );
    }
    @Test
    public void shouldConsultSuspicionsOnlyFromCurrentClusterMembers()
    {
        // Given
        InstanceId notInCluster = new InstanceId( -1 ); // backup, for example
        toTest.suspicions( notInCluster, Iterables.asSet( Iterables.iterable( instanceIds[1] ) ) );

        // When
        List<InstanceId> suspicions = toTest.getSuspicionsOf( instanceIds[1] );

        // Then
        assertThat( suspicions.size(), CoreMatchers.equalTo( 0 ) );

    }
}
