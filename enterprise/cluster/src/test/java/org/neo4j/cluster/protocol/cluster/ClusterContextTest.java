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
package org.neo4j.cluster.protocol.cluster;

import java.util.Collections;
import java.util.concurrent.Executor;

import org.junit.Test;

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
    public void testElectionVersionIsUpdatedOnElectionFromSelfAndProperlyIgnoredIfOld() throws Exception
    {
        final String coordinatorRole = "coordinator";
        final InstanceId me = new InstanceId( 1 );
        final InstanceId winner = new InstanceId( 2 );
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.<InstanceId>emptySet() );

        MultiPaxosContext multiPaxosContext = new MultiPaxosContext( me, 10, Iterables.<ElectionRole, ElectionRole
                >iterable(
                new ElectionRole( coordinatorRole ) ), mock( ClusterConfiguration.class ),
                new Executor()
                {
                    @Override
                    public void execute( Runnable command )
                    {
                        command.run();
                    }
                }, NullLogProvider.getInstance(),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class )
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
    public void testElectionVersionIsUpdatedOnElectionFromOtherAndIgnoredIfOld() throws Exception
    {
        final String coordinatorRole = "coordinator";
        final InstanceId me = new InstanceId( 1 );
        final InstanceId winner = new InstanceId( 2 );
        final InstanceId elector = new InstanceId( 2 );
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.<InstanceId>emptySet() );

        MultiPaxosContext multiPaxosContext = new MultiPaxosContext( me, 10, Iterables.<ElectionRole, ElectionRole
                >iterable(
                new ElectionRole( coordinatorRole ) ), mock( ClusterConfiguration.class ),
                new Executor()
                {
                    @Override
                    public void execute( Runnable command )
                    {
                        command.run();
                    }
                }, NullLogProvider.getInstance(),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class )
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
    public void testElectionVersionIsResetWhenElectorChangesFromMeToOther() throws Exception
    {
        final String coordinatorRole = "coordinator";
        final InstanceId me = new InstanceId( 1 );
        final InstanceId winner = new InstanceId( 2 );
        final InstanceId elector = new InstanceId( 2 );
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.<InstanceId>emptySet() );

        MultiPaxosContext multiPaxosContext = new MultiPaxosContext( me, 10, Iterables.<ElectionRole, ElectionRole
                >iterable(
                new ElectionRole( coordinatorRole ) ), mock( ClusterConfiguration.class ),
                new Executor()
                {
                    @Override
                    public void execute( Runnable command )
                    {
                        command.run();
                    }
                }, NullLogProvider.getInstance(),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class )
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
    public void testElectionVersionIsResetWhenElectorChangesFromOtherToMe() throws Exception
    {
        final String coordinatorRole = "coordinator";
        final InstanceId me = new InstanceId( 1 );
        final InstanceId winner = new InstanceId( 2 );
        final InstanceId elector = new InstanceId( 2 );
        HeartbeatContext heartbeatContext = mock(HeartbeatContext.class);
        when( heartbeatContext.getFailed() ).thenReturn( Collections.<InstanceId>emptySet() );

        MultiPaxosContext multiPaxosContext = new MultiPaxosContext( me, 10, Iterables.<ElectionRole, ElectionRole
                >iterable(
                new ElectionRole( coordinatorRole ) ), mock( ClusterConfiguration.class ),
                new Executor()
                {
                    @Override
                    public void execute( Runnable command )
                    {
                        command.run();
                    }
                }, NullLogProvider.getInstance(),
                mock( ObjectInputStreamFactory.class ), mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ), mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class )
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
