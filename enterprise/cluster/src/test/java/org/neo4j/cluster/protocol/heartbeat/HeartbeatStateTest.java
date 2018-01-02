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
package org.neo4j.cluster.protocol.heartbeat;

import java.net.URI;
import java.util.concurrent.Executor;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import org.neo4j.cluster.DelayedDirectExecutor;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.StateMachines;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.protocol.MessageArgumentMatcher;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context.MultiPaxosContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.ElectionRole;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.timeout.TimeoutStrategy;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.logging.AssertableLogProvider.inLog;

public class HeartbeatStateTest
{
    @Test
    public void shouldIgnoreSuspicionsForOurselves() throws Throwable
    {
        // Given
        InstanceId instanceId = new InstanceId( 1 );
        HeartbeatState heartbeat = HeartbeatState.heartbeat;
        ClusterConfiguration configuration = new ClusterConfiguration( "whatever", NullLogProvider.getInstance(),
                "cluster://1", "cluster://2" );
        configuration.joined( instanceId, URI.create( "cluster://1" ) );
        configuration.joined( new InstanceId( 2 ), URI.create( "cluster://2" ) );

        MultiPaxosContext context = new MultiPaxosContext( instanceId, 10, Iterables.<ElectionRole, ElectionRole>iterable(
                new ElectionRole( "coordinator" ) ), configuration,
                Mockito.mock( Executor.class ), NullLogProvider.getInstance(),
                Mockito.mock( ObjectInputStreamFactory.class ), Mockito.mock( ObjectOutputStreamFactory.class ),
                Mockito.mock( AcceptorInstanceStore.class ), Mockito.mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class ) );

        HeartbeatContext heartbeatContext = context.getHeartbeatContext();
        Message received = Message.internal( HeartbeatMessage.suspicions,
                new HeartbeatMessage.SuspicionsState( Iterables.toSet(
                        Iterables.<InstanceId, InstanceId>iterable( instanceId ) ) ) );

        received.setHeader( Message.FROM, "cluster://2" ).setHeader( Message.INSTANCE_ID, "2" );

        // When
        heartbeat.handle( heartbeatContext, received, mock( MessageHolder.class ) );

        // Then
        assertThat( heartbeatContext.getSuspicionsOf( instanceId ).size(), equalTo( 0 ) );
    }

    @Test
    public void shouldIgnoreSuspicionsForOurselvesButKeepTheRest() throws Throwable
    {
        // Given
        InstanceId myId = new InstanceId( 1 );
        InstanceId foreignId = new InstanceId( 3 );
        HeartbeatState heartbeat = HeartbeatState.heartbeat;
        ClusterConfiguration configuration = new ClusterConfiguration( "whatever", NullLogProvider.getInstance(),
                "cluster://1", "cluster://2" );
        configuration.joined( myId, URI.create( "cluster://1" ) );
        configuration.joined( new InstanceId( 2 ), URI.create( "cluster://2" ) );

        MultiPaxosContext context = new MultiPaxosContext( myId, 10, Iterables.<ElectionRole, ElectionRole>iterable(
                new ElectionRole( "coordinator" ) ), configuration,
                Mockito.mock( Executor.class ), NullLogProvider.getInstance(),
                Mockito.mock( ObjectInputStreamFactory.class ), Mockito.mock( ObjectOutputStreamFactory.class ),
                Mockito.mock( AcceptorInstanceStore.class ), Mockito.mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class ) );

        HeartbeatContext heartbeatContext = context.getHeartbeatContext();
        Message received = Message.internal( HeartbeatMessage.suspicions,
                new HeartbeatMessage.SuspicionsState( Iterables.toSet(
                        Iterables.<InstanceId, InstanceId>iterable( myId, foreignId ) ) ) );
        received.setHeader( Message.FROM, "cluster://2" ).setHeader( Message.INSTANCE_ID, "2" );

        // When
        heartbeat.handle( heartbeatContext, received, mock( MessageHolder.class ) );

        // Then
        assertThat( heartbeatContext.getSuspicionsOf( myId ).size(), equalTo( 0 ) );
        assertThat( heartbeatContext.getSuspicionsOf( foreignId ).size(), equalTo( 1 ) );
    }

    @Test
    public void shouldAddInstanceIdHeaderInCatchUpMessages() throws Throwable
    {
        // Given
        InstanceId instanceId = new InstanceId( 1 );
        HeartbeatState heartbeat = HeartbeatState.heartbeat;
        ClusterConfiguration configuration = new ClusterConfiguration( "whatever", NullLogProvider.getInstance(),
                "cluster://1", "cluster://2" );
        configuration.joined( instanceId, URI.create( "cluster://1" ) );
        InstanceId otherInstance = new InstanceId( 2 );
        configuration.joined( otherInstance, URI.create( "cluster://2" ) );

        MultiPaxosContext context = new MultiPaxosContext(
                instanceId, 10,
                Iterables.<ElectionRole, ElectionRole>iterable( new ElectionRole( "coordinator" ) ),
                configuration,
                Mockito.mock( Executor.class ),
                NullLogProvider.getInstance(),
                Mockito.mock( ObjectInputStreamFactory.class ),
                Mockito.mock( ObjectOutputStreamFactory.class ),
                Mockito.mock( AcceptorInstanceStore.class ),
                Mockito.mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class ) );

        int lastDeliveredInstanceId = 100;
        context.getLearnerContext().setLastDeliveredInstanceId( lastDeliveredInstanceId );
        // This gap will trigger the catchUp message that we'll test against
        lastDeliveredInstanceId += 20;

        HeartbeatContext heartbeatContext = context.getHeartbeatContext();
        Message received = Message.internal( HeartbeatMessage.i_am_alive,
                new HeartbeatMessage.IAmAliveState( otherInstance ) );
        received.setHeader( Message.FROM, "cluster://2" ).setHeader( Message.INSTANCE_ID, "2" )
                .setHeader( "last-learned", Integer.toString( lastDeliveredInstanceId ) );

        // When
        MessageHolder holder = mock( MessageHolder.class );
        heartbeat.handle( heartbeatContext, received, holder );

        // Then
        verify( holder, times( 1 ) ).offer( Matchers.argThat( new MessageArgumentMatcher<LearnerMessage>()
                .onMessageType( LearnerMessage.catchUp ).withHeader( Message.INSTANCE_ID, "2" ) ) );
    }

    @Test
    public void shouldLogFirstHeartbeatAfterTimeout() throws Throwable
    {
        // given
        InstanceId instanceId = new InstanceId( 1 ), otherInstance = new InstanceId( 2 );
        ClusterConfiguration configuration = new ClusterConfiguration( "whatever", NullLogProvider.getInstance(),
                "cluster://1", "cluster://2" );
        configuration.getMembers().put( otherInstance, URI.create( "cluster://2" ) );
        AssertableLogProvider internalLog = new AssertableLogProvider( true );
        TimeoutStrategy timeoutStrategy = mock( TimeoutStrategy.class );
        Timeouts timeouts = new Timeouts( timeoutStrategy );

        MultiPaxosContext context = new MultiPaxosContext(
                instanceId,
                10,
                Iterables.<ElectionRole, ElectionRole>iterable( new ElectionRole( "coordinator" ) ),
                configuration,
                mock( Executor.class ),
                internalLog,
                mock( ObjectInputStreamFactory.class ),
                mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ),
                timeouts,
                mock( ElectionCredentialsProvider.class ) );

        StateMachines stateMachines = new StateMachines(
                internalLog,
                mock( StateMachines.Monitor.class ),
                mock( MessageSource.class ),
                mock( MessageSender.class ),
                timeouts,
                mock( DelayedDirectExecutor.class ),
                new Executor()
                {
                    @Override
                    public void execute( Runnable command )
                    {
                        command.run();
                    }
                },
                instanceId );
        stateMachines.addStateMachine(
                new StateMachine( context.getHeartbeatContext(), HeartbeatMessage.class, HeartbeatState.start,
                        internalLog ) );

        timeouts.tick( 0 );
        when( timeoutStrategy.timeoutFor( any( Message.class ) ) ).thenReturn( 5l );

        // when
        stateMachines.process( Message.internal( HeartbeatMessage.join ) );
        stateMachines.process(
                Message.internal( HeartbeatMessage.i_am_alive, new HeartbeatMessage.IAmAliveState( otherInstance ) )
                        .setHeader( Message.CREATED_BY, otherInstance.toString() ) );
        for ( int i = 1; i <= 15; i++ )
        {
            timeouts.tick( i );
        }

        // then
        verify( timeoutStrategy, times( 3 ) ).timeoutTriggered( argThat( new MessageArgumentMatcher<>()
                .onMessageType( HeartbeatMessage.timed_out ) ) );
        internalLog.assertExactly(
                inLog( HeartbeatState.class ).debug( "Received timed out for server 2" ),
                inLog( HeartbeatContext.class ).info( "1(me) is now suspecting 2" ),
                inLog( HeartbeatState.class ).debug( "Received timed out for server 2" ),
                inLog( HeartbeatState.class ).debug( "Received timed out for server 2" ) );
        internalLog.clear();

        // when
        stateMachines.process(
                Message.internal( HeartbeatMessage.i_am_alive, new HeartbeatMessage.IAmAliveState( otherInstance ) )
                        .setHeader( Message.CREATED_BY, otherInstance.toString() ) );

        // then
        internalLog.assertExactly( inLog( HeartbeatState.class ).debug( "Received i_am_alive[2] after missing 3 (15ms)" ) );
    }
}
