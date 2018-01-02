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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;

public class HeartbeatIAmAliveProcessorTest
{
    @Test
    public void shouldNotCreateHeartbeatsForNonExistingInstances() throws Exception
    {
        // GIVEN
        MessageHolder outgoing = mock( MessageHolder.class );
        ClusterContext mockContext = mock( ClusterContext.class );
        ClusterConfiguration mockConfiguration = mock( ClusterConfiguration.class );
        when( mockConfiguration.getMembers() ).thenReturn(
                new HashMap<InstanceId, URI>()
            {{
                put( new InstanceId( 1 ), URI.create( "ha://1" ) );
                put( new InstanceId( 2 ), URI.create( "ha://2" ) );
            }}
        );
        when( mockContext.getConfiguration() ).thenReturn( mockConfiguration );
        HeartbeatIAmAliveProcessor processor = new HeartbeatIAmAliveProcessor( outgoing, mockContext );

        Message incoming = Message.to( mock( MessageType.class ), URI.create( "ha://someAwesomeInstanceInJapan") )
                .setHeader( Message.FROM, "some://value" ).setHeader( Message.INSTANCE_ID, "5" );

        // WHEN
        processor.process( incoming );

        // THEN
        verifyZeroInteractions( outgoing );
    }

    @Test
    public void shouldNotProcessMessagesWithEqualFromAndToHeaders() throws Exception
    {
        URI to = URI.create( "ha://someAwesomeInstanceInJapan" );

        // GIVEN
        MessageHolder outgoing = mock( MessageHolder.class );
        ClusterContext mockContext = mock( ClusterContext.class );
        ClusterConfiguration mockConfiguration = mock( ClusterConfiguration.class );
        when( mockConfiguration.getMembers() ).thenReturn(
                new HashMap<InstanceId, URI>()
                {{
                        put( new InstanceId( 1 ), URI.create( "ha://1" ) );
                        put( new InstanceId( 2 ), URI.create( "ha://2" ) );
                    }}
        );
        when( mockContext.getConfiguration() ).thenReturn( mockConfiguration );

        HeartbeatIAmAliveProcessor processor = new HeartbeatIAmAliveProcessor( outgoing, mockContext );
        Message incoming = Message.to( mock( MessageType.class ), to ).setHeader( Message.FROM, to.toASCIIString() )
                .setHeader( Message.INSTANCE_ID, "1" );

        // WHEN
        processor.process( incoming );

        // THEN
        verifyZeroInteractions( outgoing );
    }

    @Test
    public void shouldNotGenerateHeartbeatsForSuspicions() throws Exception
    {
        URI to = URI.create( "ha://1" );

        // GIVEN
        MessageHolder outgoing = mock( MessageHolder.class );
        ClusterContext mockContext = mock( ClusterContext.class );
        ClusterConfiguration mockConfiguration = mock( ClusterConfiguration.class );
        when( mockConfiguration.getMembers() ).thenReturn(
                new HashMap<InstanceId, URI>()
                {{
                        put( new InstanceId( 1 ), URI.create( "ha://1" ) );
                        put( new InstanceId( 2 ), URI.create( "ha://2" ) );
                    }}
        );
        when( mockContext.getConfiguration() ).thenReturn( mockConfiguration );

        HeartbeatIAmAliveProcessor processor = new HeartbeatIAmAliveProcessor( outgoing, mockContext );
        Message incoming = Message.to( HeartbeatMessage.suspicions , to ).setHeader( Message.FROM, to
            .toASCIIString() )
                .setHeader( Message.INSTANCE_ID, "1" );
        assertEquals( HeartbeatMessage.suspicions, incoming.getMessageType() );

        // WHEN
        processor.process( incoming );

        // THEN
        verifyZeroInteractions( outgoing );
    }

    @Test
    public void shouldNotGenerateHeartbeatsForHeartbeats() throws Exception
    {
        URI to = URI.create( "ha://1" );

        // GIVEN
        MessageHolder outgoing = mock( MessageHolder.class );
        ClusterContext mockContext = mock( ClusterContext.class );
        ClusterConfiguration mockConfiguration = mock( ClusterConfiguration.class );
        when( mockConfiguration.getMembers() ).thenReturn(
                new HashMap<InstanceId, URI>()
                {{
                        put( new InstanceId( 1 ), URI.create( "ha://1" ) );
                        put( new InstanceId( 2 ), URI.create( "ha://2" ) );
                    }}
        );
        when( mockContext.getConfiguration() ).thenReturn( mockConfiguration );

        HeartbeatIAmAliveProcessor processor = new HeartbeatIAmAliveProcessor( outgoing, mockContext );
        Message incoming = Message.to( HeartbeatMessage.i_am_alive , to ).setHeader( Message.FROM, to
                .toASCIIString() )
                .setHeader( Message.INSTANCE_ID, "1" );
        assertEquals( HeartbeatMessage.i_am_alive, incoming.getMessageType() );

        // WHEN
        processor.process( incoming );

        // THEN
        verifyZeroInteractions( outgoing );
    }

    @Test
    public void shouldCorrectlySetTheInstanceIdHeaderInTheGeneratedHeartbeat() throws Exception
    {
        final List<Message> sentOut = new LinkedList<Message>();

        // Given
        MessageHolder holder = mock( MessageHolder.class );
        // The sender, which adds messages outgoing to the list above.
        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                sentOut.add( (Message) invocation.getArguments()[0] );
                return null;
            }
        } ).when( holder ).offer( Matchers.<Message<MessageType>>any() );

        ClusterContext mockContext = mock( ClusterContext.class );
        ClusterConfiguration mockConfiguration = mock( ClusterConfiguration.class );
        when( mockConfiguration.getMembers() ).thenReturn(
                new HashMap<InstanceId, URI>()
                {{
                        put( new InstanceId( 1 ), URI.create( "ha://1" ) );
                        put( new InstanceId( 2 ), URI.create( "ha://2" ) );
                    }}
        );
        when( mockContext.getConfiguration() ).thenReturn( mockConfiguration );

        HeartbeatIAmAliveProcessor processor = new HeartbeatIAmAliveProcessor( holder, mockContext );

        Message incoming = Message.to( mock( MessageType.class ), URI.create( "ha://someAwesomeInstanceInJapan") )
                .setHeader( Message.INSTANCE_ID, "2" ).setHeader( Message.FROM, "ha://2" );

        // WHEN
        processor.process( incoming );

        // THEN
        assertEquals( 1, sentOut.size() );
        assertEquals( HeartbeatMessage.i_am_alive, sentOut.get( 0 ).getMessageType() );
        assertEquals( new InstanceId( 2 ), ((HeartbeatMessage.IAmAliveState) sentOut.get( 0 ).getPayload() ).getServer() );
    }

    /*
     * This test is required to ensure compatibility with the previous version. If we fail on non existing INSTANCE_ID
     * header then heartbeats may pause during rolling upgrades and cause timeouts, which we don't want.
     */
    @Test
    public void shouldRevertToInverseUriLookupIfNoInstanceIdHeader() throws Exception
    {
        final List<Message> sentOut = new LinkedList<Message>();
        String instance2UriString = "ha://2";

        // Given
        MessageHolder holder = mock( MessageHolder.class );
        // The sender, which adds messages outgoing to the list above.
        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                sentOut.add( (Message) invocation.getArguments()[0] );
                return null;
            }
        } ).when( holder ).offer( Matchers.<Message<MessageType>>any() );

        ClusterContext mockContext = mock( ClusterContext.class );
        ClusterConfiguration mockConfiguration = mock( ClusterConfiguration.class );
        when( mockConfiguration.getIdForUri( URI.create( instance2UriString ) ) ).thenReturn( new InstanceId( 2 ) );
        when( mockConfiguration.getMembers() ).thenReturn(
                new HashMap<InstanceId, URI>()
                {{
                        put( new InstanceId( 1 ), URI.create( "ha://1" ) );
                        put( new InstanceId( 2 ), URI.create( "ha://2" ) );
                    }}
        );
        when( mockContext.getConfiguration() ).thenReturn( mockConfiguration );

        HeartbeatIAmAliveProcessor processor = new HeartbeatIAmAliveProcessor( holder, mockContext );

        Message incoming = Message.to( mock( MessageType.class ), URI.create( "ha://someAwesomeInstanceInJapan") )
                .setHeader( Message.FROM, instance2UriString );

        // WHEN
        processor.process( incoming );

        // THEN
        assertEquals( 1, sentOut.size() );
        assertEquals( HeartbeatMessage.i_am_alive, sentOut.get( 0 ).getMessageType() );
        assertEquals( new InstanceId( 2 ), ((HeartbeatMessage.IAmAliveState) sentOut.get( 0 ).getPayload() ).getServer() );
    }
}
