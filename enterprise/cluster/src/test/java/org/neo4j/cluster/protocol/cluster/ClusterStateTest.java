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

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.ArgumentMatcher;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.com.message.TrackingMessageHolder;
import org.neo4j.cluster.protocol.cluster.ClusterMessage.ConfigurationRequestState;
import org.neo4j.cluster.protocol.cluster.ClusterMessage.ConfigurationResponseState;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.cluster.com.message.Message.DISCOVERED;
import static org.neo4j.cluster.com.message.Message.internal;
import static org.neo4j.cluster.com.message.Message.to;
import static org.neo4j.cluster.protocol.cluster.ClusterMessage.configurationRequest;
import static org.neo4j.cluster.protocol.cluster.ClusterMessage.configurationTimeout;
import static org.neo4j.cluster.protocol.cluster.ClusterMessage.joinDenied;

public class ClusterStateTest
{
    @Test
    public void joinDeniedResponseShouldContainRespondersConfiguration() throws Throwable
    {
        // GIVEN
        ClusterContext context = mock( ClusterContext.class );
        Map<InstanceId, URI> existingMembers = members( 1, 2 );
        when( context.isCurrentlyAlive( any( InstanceId.class ) ) ).thenReturn( true );
        when( context.getMembers() ).thenReturn( existingMembers );
        when( context.getConfiguration() ).thenReturn( clusterConfiguration( existingMembers ) );
        when( context.getLog( any( Class.class ) ) ).thenReturn( NullLog.getInstance() );
        TrackingMessageHolder outgoing = new TrackingMessageHolder();
        Message<ClusterMessage> message = to( configurationRequest, uri( 1 ), configuration( 2 ) )
                .setHeader( Message.HEADER_FROM, uri( 2 ).toString() );

        // WHEN an instance responds to a join request, responding that the joining instance cannot join
        ClusterState.entered.handle( context, message, outgoing );

        // THEN assert that the responding instance sends its configuration along with the response
        Message<ClusterMessage> response = outgoing.single();
        assertTrue( response.getPayload() instanceof ConfigurationResponseState );
        ConfigurationResponseState responseState = response.getPayload();
        assertEquals( existingMembers, responseState.getMembers() );
    }

    @Test
    public void joinDeniedHandlingShouldKeepResponseConfiguration() throws Throwable
    {
        // GIVEN
        ClusterContext context = mock( ClusterContext.class );
        when( context.getLog( any( Class.class ) ) ).thenReturn( NullLog.getInstance() );
        TrackingMessageHolder outgoing = new TrackingMessageHolder();
        Map<InstanceId, URI> members = members( 1, 2 );

        // WHEN a joining instance receives a denial to join
        ClusterState.discovery.handle( context, to( joinDenied, uri( 2 ),
                configurationResponseState( members ) ), outgoing );

        // THEN assert that the response contains the configuration
        verify( context ).joinDenied( argThat(
                new ConfigurationResponseStateMatcher().withMembers( members ) ) );
    }

    @Test
    public void joinDeniedTimeoutShouldBeHandledWithExceptionIncludingConfiguration() throws Throwable
    {
        // GIVEN
        ClusterContext context = mock( ClusterContext.class );
        Map<InstanceId, URI> existingMembers = members( 1, 2 );
        when( context.getLog( any( Class.class ) ) ).thenReturn( NullLog.getInstance() );
        when( context.getJoiningInstances() ).thenReturn( Collections.emptyList() );
        when( context.hasJoinBeenDenied() ).thenReturn( true );
        when( context.getJoinDeniedConfigurationResponseState() )
                .thenReturn( configurationResponseState( existingMembers ) );
        TrackingMessageHolder outgoing = new TrackingMessageHolder();

        // WHEN the join denial actually takes effect (signaled by a join timeout locally)
        ClusterState.joining.handle( context, to( ClusterMessage.joiningTimeout, uri( 2 ) )
                .setHeader( Message.HEADER_CONVERSATION_ID, "bla" ), outgoing );

        // THEN assert that the failure contains the received configuration
        Message<? extends MessageType> response = outgoing.single();
        ClusterEntryDeniedException deniedException = response.getPayload();
        assertEquals( existingMembers, deniedException.getConfigurationResponseState().getMembers() );
    }

    @Test
    public void shouldNotDenyJoinToInstanceThatRejoinsBeforeTimingOut() throws Throwable
    {
        // GIVEN
        ClusterContext context = mock( ClusterContext.class );
        Map<InstanceId, URI> existingMembers = members( 1, 2 );
        when( context.isCurrentlyAlive( id( 2 ) ) ).thenReturn( true );
        when( context.getMembers() ).thenReturn( existingMembers );
        when( context.getConfiguration() ).thenReturn( clusterConfiguration( existingMembers ) );
        when( context.getLog( any( Class.class ) ) ).thenReturn( NullLog.getInstance() );
        when( context.getUriForId( id( 2 ) ) ).thenReturn( uri( 2 ) );
        TrackingMessageHolder outgoing = new TrackingMessageHolder();
        Message<ClusterMessage> message = to( configurationRequest, uri( 1 ), configuration( 2 ) )
                .setHeader( Message.HEADER_FROM, uri( 2 ).toString() );

        // WHEN the join denial actually takes effect (signaled by a join timeout locally)
        ClusterState.entered.handle( context, message, outgoing );

        // THEN assert that the failure contains the received configuration
        Message<? extends MessageType> response = outgoing.single();
        assertEquals( ClusterMessage.configurationResponse, response.getMessageType() );
    }

    @Test
    public void discoveredInstancesShouldBeOnlyOnesWeHaveContactedDirectly() throws Throwable
    {
        // GIVEN
        ClusterContext context = mock( ClusterContext.class );
        when( context.getLog( any( Class.class ) ) ).thenReturn( NullLog.getInstance() );
        when( context.getUriForId( id( 2 ) ) ).thenReturn( uri( 2 ) );

        List<ConfigurationRequestState> discoveredInstances = new LinkedList<>();
        when( context.getDiscoveredInstances() ).thenReturn( discoveredInstances );
        when( context.shouldFilterContactingInstances() ).thenReturn( true );

        MessageHolder outgoing = mock( MessageHolder.class );
        ConfigurationRequestState configurationRequestFromTwo = configuration( 2 );
        Message<ClusterMessage> message = to( configurationRequest, uri( 1 ), configurationRequestFromTwo )
                .setHeader( Message.HEADER_FROM, uri( 2 ).toString() );

        // WHEN
        // We receive a configuration request from an instance which we haven't contacted
        ClusterState.discovery.handle( context, message, outgoing );

        // THEN
        // It shouldn't be added to the discovered instances
        assertTrue( discoveredInstances.isEmpty() );

        // WHEN
        // It subsequently contacts us
        when( context.haveWeContactedInstance( configurationRequestFromTwo ) ).thenReturn( true );
        ClusterState.discovery.handle( context, message, outgoing );

        // Then
        assertTrue( discoveredInstances.contains( configurationRequestFromTwo ) );
    }

    @Test
    public void discoveredInstancesShouldNotFilterByDefault() throws Throwable
    {
        // GIVEN
        ClusterContext context = mock( ClusterContext.class );
        when( context.getLog( any( Class.class ) ) ).thenReturn( NullLog.getInstance() );
        when( context.getUriForId( id( 2 ) ) ).thenReturn( uri( 2 ) );
        when( context.getUriForId( id( 3 ) ) ).thenReturn( uri( 3 ) );

        List<ConfigurationRequestState> discoveredInstances = new LinkedList<>();
        when( context.getDiscoveredInstances() ).thenReturn( discoveredInstances );

        MessageHolder outgoing = mock( MessageHolder.class );
        ConfigurationRequestState configurationRequestFromTwo = configuration( 2 );
        Message<ClusterMessage> messageFromTwo = to( configurationRequest, uri( 1 ), configurationRequestFromTwo )
                .setHeader( Message.HEADER_FROM, uri( 2 ).toString() );
        ConfigurationRequestState configurationRequestFromThree = configuration( 3 );
        Message<ClusterMessage> messageFromThree = to( configurationRequest, uri( 1 ), configurationRequestFromThree )
                .setHeader( Message.HEADER_FROM, uri( 3 ).toString() );

        // WHEN
        // We receive a configuration request from an instance which we haven't contacted
        ClusterState.discovery.handle( context, messageFromTwo, outgoing );

        // THEN
        // Since the setting is on, it should be added to the list anyway
        assertTrue( discoveredInstances.contains( configurationRequestFromTwo ) );

        // WHEN
        // Another contacts us as well
        ClusterState.discovery.handle( context, messageFromThree, outgoing );

        // Then
        // That should be in as well
        assertTrue( discoveredInstances.contains( configurationRequestFromTwo ) );
        assertTrue( discoveredInstances.contains( configurationRequestFromThree ) );
    }

    @Test
    public void shouldSetDiscoveryHeaderProperly() throws Throwable
    {
        // GIVEN
        ClusterContext context = mock( ClusterContext.class );
        when( context.getLog( any( Class.class ) ) ).thenReturn( NullLog.getInstance() );
        when( context.getUriForId( id( 2 ) ) ).thenReturn( uri( 2 ) );
        when( context.getJoiningInstances() ).thenReturn( singletonList( uri( 2 ) ) );

        List<ConfigurationRequestState> discoveredInstances = new LinkedList<>();
        when( context.getDiscoveredInstances() ).thenReturn( discoveredInstances );

        TrackingMessageHolder outgoing = new TrackingMessageHolder();
        ClusterMessage.ConfigurationTimeoutState timeoutState = new ClusterMessage.ConfigurationTimeoutState( 3 );
        Message<ClusterMessage> message = internal( configurationTimeout, timeoutState );
        String discoveryHeader = "1,2,3";
        when( context.generateDiscoveryHeader() ).thenReturn( discoveryHeader );

        // WHEN
        // We receive a configuration request from an instance which we haven't contacted
        ClusterState.discovery.handle( context, message, outgoing );

        // THEN
        // It shouldn't be added to the discovered instances
        assertEquals( discoveryHeader, outgoing.first().getHeader( DISCOVERED ) );
    }

    private ConfigurationResponseState configurationResponseState( Map<InstanceId, URI> existingMembers )
    {
        return new ConfigurationResponseState( Collections.emptyMap(), existingMembers, null,
                Collections.emptySet(),  "ClusterStateTest" );
    }

    private ClusterConfiguration clusterConfiguration( Map<InstanceId, URI> members )
    {
        ClusterConfiguration config = new ClusterConfiguration( "ClusterStateTest", NullLogProvider.getInstance() );
        config.setMembers( members );
        return config;
    }

    private Map<InstanceId,URI> members( int... memberIds )
    {
        Map<InstanceId,URI> members = new HashMap<>();
        for ( int memberId : memberIds )
        {
            members.put( new InstanceId( memberId ), uri( memberId ) );
        }
        return members;
    }

    private ConfigurationRequestState configuration( int joiningInstance )
    {
        return new ConfigurationRequestState( new InstanceId( joiningInstance ), uri( joiningInstance ) );
    }

    private URI uri( int i )
    {
        return URI.create( "http://localhost:" + (6000 + i) + "?serverId=" + i );
    }

    private InstanceId id( int i )
    {
        return new InstanceId( i );
    }

    private static class ConfigurationResponseStateMatcher implements ArgumentMatcher<ConfigurationResponseState>
    {
        private Map<InstanceId, URI> members;

        public ConfigurationResponseStateMatcher withMembers( Map<InstanceId, URI> members )
        {
            this.members = members;
            return this;
        }

        @Override
        public boolean matches( ConfigurationResponseState argument )
        {
            return argument.getMembers().equals( this.members );
        }
    }
}
