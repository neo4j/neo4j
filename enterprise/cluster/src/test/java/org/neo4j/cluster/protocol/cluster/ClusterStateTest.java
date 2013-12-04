/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.ArgumentMatcher;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.cluster.ClusterMessage.ConfigurationRequestState;
import org.neo4j.cluster.protocol.cluster.ClusterMessage.ConfigurationResponseState;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.cluster.com.message.Message.to;
import static org.neo4j.cluster.protocol.cluster.ClusterMessage.configurationRequest;
import static org.neo4j.cluster.protocol.cluster.ClusterMessage.joinDenied;
import static org.neo4j.helpers.collection.IteratorUtil.single;

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
        when( context.getLogger( any( Class.class ) ) ).thenReturn( StringLogger.DEV_NULL );
        TrackingMessageHolder outgoing = new TrackingMessageHolder();
        Message<ClusterMessage> message = to( configurationRequest, uri( 1 ), configuration( 2 ) )
                .setHeader( Message.FROM, uri( 2 ).toString() );
        
        // WHEN an instance responds to a join request, responding that the joining instance cannot join
        ClusterState.entered.handle( context, message, outgoing );
        
        // THEN assert that the responding instance sends its configuration along with the response
        @SuppressWarnings( "unchecked" )
        Message<ClusterMessage> response = (Message<ClusterMessage>) single( outgoing.messages );
        assertTrue( response.getPayload() instanceof ConfigurationResponseState );
        ConfigurationResponseState responseState = response.getPayload();
        assertEquals( existingMembers, responseState.getMembers() );
    }

    @Test
    public void joinDeniedHandlingShouldKeepResponseConfiguration() throws Throwable
    {
        // GIVEN
        ClusterContext context = mock( ClusterContext.class );
        when( context.getLogger( any( Class.class ) ) ).thenReturn( StringLogger.DEV_NULL );
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
        when( context.getLogger( any( Class.class ) ) ).thenReturn( StringLogger.DEV_NULL );
        when( context.getJoiningInstances() ).thenReturn( Collections.<URI>emptyList() );
        when( context.hasJoinBeenDenied() ).thenReturn( true );
        when( context.getJoinDeniedConfigurationResponseState() )
                .thenReturn( configurationResponseState( existingMembers ) );
        TrackingMessageHolder outgoing = new TrackingMessageHolder();
        
        // WHEN the join denial actually takes effect (signaled by a join timeout locally)
        ClusterState.joining.handle( context, to( ClusterMessage.joiningTimeout, uri( 2 ) )
                .setHeader( Message.CONVERSATION_ID, "bla" ), outgoing );
        
        // THEN assert that the failure contains the received configuration
        Message<? extends MessageType> response = single( outgoing.messages );
        ClusterEntryDeniedException deniedException = response.getPayload();
        assertEquals( existingMembers, deniedException.getConfigurationResponseState().getMembers() );
    }
    
    private ConfigurationResponseState configurationResponseState( Map<InstanceId, URI> existingMembers )
    {
        return new ConfigurationResponseState( Collections.<String,InstanceId>emptyMap(),
                existingMembers, null, "ClusterStateTest" );
    }

    private ClusterConfiguration clusterConfiguration( Map<InstanceId, URI> members )
    {
        ClusterConfiguration config = new ClusterConfiguration( "ClusterStateTest", StringLogger.DEV_NULL );
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
        return URI.create( "http://localhost:" + (6000+i) + "?serverId=" + i );
    }

    public static class TrackingMessageHolder implements MessageHolder
    {
        private final List<Message<? extends MessageType>> messages = new ArrayList<>();
        
        @Override
        public void offer( Message<? extends MessageType> message )
        {
            messages.add( message );
        }
    }

    private static class ConfigurationResponseStateMatcher extends ArgumentMatcher<ConfigurationResponseState>
    {
        private Map<InstanceId, URI> members;

        public ConfigurationResponseStateMatcher withMembers( Map<InstanceId, URI> members )
        {
            this.members = members;
            return this;
        }

        @Override
        public boolean matches( Object argument )
        {
            ConfigurationResponseState arg = (ConfigurationResponseState) argument;
            return arg.getMembers().equals( this.members );
        }
    }
}
