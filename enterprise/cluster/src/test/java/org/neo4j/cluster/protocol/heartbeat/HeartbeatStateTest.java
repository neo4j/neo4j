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
package org.neo4j.cluster.protocol.heartbeat;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.net.URI;
import java.util.concurrent.Executor;

import org.neo4j.cluster.ClusterSettings;
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
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.asSet;
import static org.neo4j.helpers.collection.Iterables.iterable;
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

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext context = new MultiPaxosContext( instanceId, iterable(
                new ElectionRole( "coordinator" ) ), configuration,
                Mockito.mock( Executor.class ), NullLogProvider.getInstance(),
                Mockito.mock( ObjectInputStreamFactory.class ), Mockito.mock( ObjectOutputStreamFactory.class ),
                Mockito.mock( AcceptorInstanceStore.class ), Mockito.mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class ), config );

        HeartbeatContext heartbeatContext = context.getHeartbeatContext();
        Message received = Message.internal( HeartbeatMessage.suspicions,
                new HeartbeatMessage.SuspicionsState( asSet( iterable( instanceId ) ) ) );
        received.setHeader( Message.HEADER_FROM, "cluster://2" ).setHeader( Message.HEADER_INSTANCE_ID, "2" );

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

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext context = new MultiPaxosContext( myId, iterable(
                new ElectionRole( "coordinator" ) ), configuration,
                Mockito.mock( Executor.class ), NullLogProvider.getInstance(),
                Mockito.mock( ObjectInputStreamFactory.class ), Mockito.mock( ObjectOutputStreamFactory.class ),
                Mockito.mock( AcceptorInstanceStore.class ), Mockito.mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class ), config );

        HeartbeatContext heartbeatContext = context.getHeartbeatContext();
        Message received = Message.internal( HeartbeatMessage.suspicions,
                new HeartbeatMessage.SuspicionsState( asSet( iterable( myId, foreignId ) ) ) );
        received.setHeader( Message.HEADER_FROM, "cluster://2" ).setHeader( Message.HEADER_INSTANCE_ID, "2" );

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

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext context = new MultiPaxosContext(
                instanceId,
                iterable( new ElectionRole( "coordinator" ) ),
                configuration,
                Mockito.mock( Executor.class ),
                NullLogProvider.getInstance(),
                Mockito.mock( ObjectInputStreamFactory.class ),
                Mockito.mock( ObjectOutputStreamFactory.class ),
                Mockito.mock( AcceptorInstanceStore.class ),
                Mockito.mock( Timeouts.class ),
                mock( ElectionCredentialsProvider.class ),
                config );

        int lastDeliveredInstanceId = 100;
        context.getLearnerContext().setLastDeliveredInstanceId( lastDeliveredInstanceId );
        // This gap will trigger the catchUp message that we'll test against
        lastDeliveredInstanceId += 20;

        HeartbeatContext heartbeatContext = context.getHeartbeatContext();
        Message received = Message.internal( HeartbeatMessage.i_am_alive,
                new HeartbeatMessage.IAmAliveState( otherInstance ) );
        received.setHeader( Message.HEADER_FROM, "cluster://2" ).setHeader( Message.HEADER_INSTANCE_ID, "2" )
                .setHeader( "last-learned", Integer.toString( lastDeliveredInstanceId ) );

        // When
        MessageHolder holder = mock( MessageHolder.class );
        heartbeat.handle( heartbeatContext, received, holder );

        // Then
        verify( holder, times( 1 ) ).offer( ArgumentMatchers.argThat( new MessageArgumentMatcher<LearnerMessage>()
                .onMessageType( LearnerMessage.catchUp ).withHeader( Message.HEADER_INSTANCE_ID, "2" ) ) );
    }

    @Test
    public void shouldLogFirstHeartbeatAfterTimeout()
    {
        // given
        InstanceId instanceId = new InstanceId( 1 );
        InstanceId otherInstance = new InstanceId( 2 );
        ClusterConfiguration configuration = new ClusterConfiguration( "whatever", NullLogProvider.getInstance(),
                "cluster://1", "cluster://2" );
        configuration.getMembers().put( otherInstance, URI.create( "cluster://2" ) );
        AssertableLogProvider internalLog = new AssertableLogProvider( true );
        TimeoutStrategy timeoutStrategy = mock( TimeoutStrategy.class );
        Timeouts timeouts = new Timeouts( timeoutStrategy );

        Config config = mock( Config.class );
        when( config.get( ClusterSettings.max_acceptors ) ).thenReturn( 10 );

        MultiPaxosContext context = new MultiPaxosContext(
                instanceId,
                iterable( new ElectionRole( "coordinator" ) ),
                configuration,
                mock( Executor.class ),
                internalLog,
                mock( ObjectInputStreamFactory.class ),
                mock( ObjectOutputStreamFactory.class ),
                mock( AcceptorInstanceStore.class ),
                timeouts,
                mock( ElectionCredentialsProvider.class ),
                config );

        StateMachines stateMachines = new StateMachines(
                internalLog,
                mock( StateMachines.Monitor.class ),
                mock( MessageSource.class ),
                mock( MessageSender.class ),
                timeouts,
                mock( DelayedDirectExecutor.class ),
                Runnable::run,
                instanceId );
        stateMachines.addStateMachine(
                new StateMachine( context.getHeartbeatContext(), HeartbeatMessage.class, HeartbeatState.start,
                        internalLog ) );

        timeouts.tick( 0 );
        when( timeoutStrategy.timeoutFor( any( Message.class ) ) ).thenReturn( 5L );

        // when
        stateMachines.process( Message.internal( HeartbeatMessage.join ) );
        stateMachines.process(
                Message.internal( HeartbeatMessage.i_am_alive, new HeartbeatMessage.IAmAliveState( otherInstance ) )
                        .setHeader( Message.HEADER_CREATED_BY, otherInstance.toString() ) );
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
                        .setHeader( Message.HEADER_CREATED_BY, otherInstance.toString() ) );

        // then
        internalLog.assertExactly( inLog( HeartbeatState.class ).debug( "Received i_am_alive[2] after missing 3 (15ms)" ) );
    }
}
