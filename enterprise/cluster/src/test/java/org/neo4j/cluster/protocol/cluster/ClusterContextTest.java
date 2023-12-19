/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster.protocol.cluster;

import org.junit.Test;

import java.util.Collections;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context.MultiPaxosContext;
import org.neo4j.cluster.protocol.election.ElectionContext;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.ElectionRole;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ClusterContextTest
{
    @Test
    public void testElectionVersionIsUpdatedOnElectionFromSelfAndProperlyIgnoredIfOld()
    {
        final String coordinatorRole = "coordinator";
        final InstanceId me = new InstanceId( 1 );
        final InstanceId winner = new InstanceId( 2 );
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.emptySet() );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext multiPaxosContext = new MultiPaxosContext( me, Iterables.iterable(
                new ElectionRole( coordinatorRole ) ), mock( ClusterConfiguration.class ), Runnable::run, NullLogProvider.getInstance(),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class ),
                config
        );
        ClusterContext context = multiPaxosContext.getClusterContext();
        ElectionContext electionContext = multiPaxosContext.getElectionContext();

        ClusterListener listener = mock( ClusterListener.class );
        context.addClusterListener( listener );

        electionContext.forgetElection( coordinatorRole );
        long expectedVersion = electionContext.newConfigurationStateChange().getVersion();
        context.elected( coordinatorRole, winner, me, expectedVersion );
        assertEquals( 1, expectedVersion );
        verify( listener, times(1) ).elected( coordinatorRole, winner, null );

        electionContext.forgetElection( coordinatorRole );
        expectedVersion = electionContext.newConfigurationStateChange().getVersion();
        context.elected( coordinatorRole, winner, me, expectedVersion );
        assertEquals( 2, expectedVersion );
        verify( listener, times(2) ).elected( coordinatorRole, winner, null );

        context.elected( coordinatorRole, winner, me, expectedVersion - 1  );
        verifyNoMoreInteractions( listener );
    }

    @Test
    public void testElectionVersionIsUpdatedOnElectionFromOtherAndIgnoredIfOld()
    {
        final String coordinatorRole = "coordinator";
        final InstanceId me = new InstanceId( 1 );
        final InstanceId winner = new InstanceId( 2 );
        final InstanceId elector = new InstanceId( 2 );
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.emptySet() );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext multiPaxosContext = new MultiPaxosContext( me, Iterables.iterable(
                new ElectionRole( coordinatorRole ) ), mock( ClusterConfiguration.class ), Runnable::run, NullLogProvider.getInstance(),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class ),
                config
        );
        ClusterContext context = multiPaxosContext.getClusterContext();

        ClusterListener listener = mock( ClusterListener.class );
        context.addClusterListener( listener );

        context.elected( coordinatorRole, winner, elector, 2 );
        verify( listener, times(1) ).elected( coordinatorRole, winner, null );

        context.elected( coordinatorRole, winner, elector, 3 );
        verify( listener, times(2) ).elected( coordinatorRole, winner, null );

        context.elected( coordinatorRole, winner, elector, 2 );
        verifyNoMoreInteractions( listener );
    }

    @Test
    public void testElectionVersionIsResetWhenElectorChangesFromMeToOther()
    {
        final String coordinatorRole = "coordinator";
        final InstanceId me = new InstanceId( 1 );
        final InstanceId winner = new InstanceId( 2 );
        final InstanceId elector = new InstanceId( 2 );
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.emptySet() );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext multiPaxosContext = new MultiPaxosContext( me, Iterables.iterable(
                new ElectionRole( coordinatorRole ) ), mock( ClusterConfiguration.class ), Runnable::run, NullLogProvider.getInstance(),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class ), config
        );
        ClusterContext context = multiPaxosContext.getClusterContext();
        ElectionContext electionContext = multiPaxosContext.getElectionContext();

        ClusterListener listener = mock( ClusterListener.class );
        context.setLastElectorVersion( 5 );
        context.setLastElector( me );
        context.addClusterListener( listener );

        long expectedVersion = electionContext.newConfigurationStateChange().getVersion();
        context.elected( coordinatorRole, winner, me, expectedVersion );
        verify( listener, times(1) ).elected( coordinatorRole, winner, null );

        context.elected( coordinatorRole, winner, elector, 2 );
        verify( listener, times(2) ).elected( coordinatorRole, winner, null );

        context.elected( coordinatorRole, winner, elector, 3 );
        verify( listener, times(3) ).elected( coordinatorRole, winner, null );

        context.elected( coordinatorRole, winner, elector, 2 );
        verifyNoMoreInteractions( listener );
    }

    @Test
    public void testElectionVersionIsResetWhenElectorChangesFromOtherToMe()
    {
        final String coordinatorRole = "coordinator";
        final InstanceId me = new InstanceId( 1 );
        final InstanceId winner = new InstanceId( 2 );
        final InstanceId elector = new InstanceId( 2 );
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.emptySet() );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext multiPaxosContext = new MultiPaxosContext( me, Iterables.iterable(
                new ElectionRole( coordinatorRole ) ), mock( ClusterConfiguration.class ), Runnable::run, NullLogProvider.getInstance(),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class ), config
        );
        ClusterContext context = multiPaxosContext.getClusterContext();
        ElectionContext electionContext = multiPaxosContext.getElectionContext();

        ClusterListener listener = mock( ClusterListener.class );
        context.setLastElectorVersion( 5 );
        context.setLastElector( elector );
        context.addClusterListener( listener );

        context.elected( coordinatorRole, winner, elector, 6 );
        verify( listener, times(1) ).elected( coordinatorRole, winner, null );

        electionContext.forgetElection( coordinatorRole );
        long expectedVersion = electionContext.newConfigurationStateChange().getVersion();
        context.elected( coordinatorRole, winner, me, expectedVersion );
        verify( listener, times(2) ).elected( coordinatorRole, winner, null );
    }
}
