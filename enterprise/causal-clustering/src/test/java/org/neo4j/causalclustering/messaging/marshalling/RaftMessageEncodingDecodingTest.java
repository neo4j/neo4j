/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.roles.AppendEntriesRequestBuilder;
import org.neo4j.causalclustering.core.consensus.roles.AppendEntriesResponseBuilder;
import org.neo4j.causalclustering.core.consensus.vote.VoteRequestBuilder;
import org.neo4j.causalclustering.core.consensus.vote.VoteResponseBuilder;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RaftMessageEncodingDecodingTest
{
    private ClusterId clusterId = new ClusterId( UUID.randomUUID() );

    @Test
    public void shouldSerializeAppendRequestWithMultipleEntries() throws Exception
    {
        MemberId sender = new MemberId( UUID.randomUUID() );
        RaftMessages.AppendEntries.Request request = new AppendEntriesRequestBuilder()
                .from( sender )
                .leaderCommit( 2 )
                .leaderTerm( 4 )
                .logEntry( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) ) )
                .logEntry( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 3 ) ) )
                .logEntry( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 4 ) ) ).build();
        serializeReadBackAndVerifyMessage( request );
    }

    @Test
    public void shouldSerializeAppendRequestWithNoEntries() throws Exception
    {
        MemberId sender = new MemberId( UUID.randomUUID() );
        RaftMessages.AppendEntries.Request request = new AppendEntriesRequestBuilder()
                .from( sender )
                .leaderCommit( 2 )
                .leaderTerm( 4 )
                .build();
        serializeReadBackAndVerifyMessage( request );
    }

    @Test
    public void shouldSerializeAppendResponse() throws Exception
    {
        MemberId sender = new MemberId( UUID.randomUUID() );
        RaftMessages.AppendEntries.Response request = new AppendEntriesResponseBuilder()
                .from( sender )
                .success()
                .matchIndex( 12 )
                .build();
        serializeReadBackAndVerifyMessage( request );
    }

    @Test
    public void shouldSerializeHeartbeats() throws Exception
    {
        // Given
        Instant now = Instant.now();
        Clock clock = Clock.fixed( now, ZoneOffset.UTC );
        RaftMessageEncoder encoder = new RaftMessageEncoder( marshal );
        RaftMessageDecoder decoder = new RaftMessageDecoder( marshal, clock );

        // Deserialization adds read objects in this list
        ArrayList<Object> thingsRead = new ArrayList<>( 1 );

        // When
        MemberId sender = new MemberId( UUID.randomUUID() );
        RaftMessages.ClusterIdAwareMessage message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, clusterId,
        new RaftMessages.Heartbeat( sender, 1, 2, 3 ) );
        ChannelHandlerContext ctx = setupContext();
        ByteBuf buffer = null;
        try
        {
            buffer = ctx.alloc().buffer();
            encoder.encode( ctx, message, buffer );

            // When
            decoder.decode( null, buffer, thingsRead );

            // Then
            assertEquals( 1, thingsRead.size() );
            assertEquals( message, thingsRead.get( 0 ) );
        }
        finally
        {
            if ( buffer != null )
            {
                buffer.release();
            }
        }
    }

    @Test
    public void shouldSerializeVoteRequest() throws Exception
    {
        MemberId sender = new MemberId( UUID.randomUUID() );
        RaftMessages.Vote.Request request = new VoteRequestBuilder()
                .candidate( sender )
                .from( sender )
                .lastLogIndex( 2 )
                .lastLogTerm( 1 )
                .term( 3 )
                .build();
        serializeReadBackAndVerifyMessage( request );
    }

    @Test
    public void shouldSerializeVoteResponse() throws Exception
    {
        MemberId sender = new MemberId( UUID.randomUUID() );
        RaftMessages.Vote.Response request = new VoteResponseBuilder()
                .from( sender )
                .grant()
                .term( 3 )
                .build();
        serializeReadBackAndVerifyMessage( request );
    }

    private void serializeReadBackAndVerifyMessage( RaftMessages.RaftMessage message ) throws Exception
    {
        // Given
        Instant now = Instant.now();
        Clock clock = Clock.fixed( now, ZoneOffset.UTC );
        RaftMessageEncoder encoder = new RaftMessageEncoder( marshal );
        RaftMessageDecoder decoder = new RaftMessageDecoder( marshal, clock );

        // Deserialization adds read objects in this list
        ArrayList<Object> thingsRead = new ArrayList<>( 1 );

        // When
        RaftMessages.ClusterIdAwareMessage decoratedMessage =
                RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, clusterId, message );
        ChannelHandlerContext ctx = setupContext();
        ByteBuf buffer = null;
        try
        {
            buffer = ctx.alloc().buffer();
            encoder.encode( ctx, decoratedMessage, buffer );

            // When
            decoder.decode( null, buffer, thingsRead );

            // Then
            assertEquals( 1, thingsRead.size() );
            assertEquals( decoratedMessage, thingsRead.get( 0 ) );
        }
        finally
        {
            if ( buffer != null )
            {
                buffer.release();
            }
        }
    }

    private static ChannelHandlerContext setupContext()
    {
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );
        when( context.alloc() ).thenReturn( ByteBufAllocator.DEFAULT );
        return context;
    }

    /*
     * Serializer for ReplicatedIntegers. Differs form the one in RaftMessageProcessingTest in that it does not
     * assume that there is only a single entry in the stream, which allows for asserting no remaining bytes once the
     * first entry is read from the buffer.
     */
    private static final ChannelMarshal<ReplicatedContent> marshal = new SafeChannelMarshal<ReplicatedContent>()
    {
        @Override
        public void marshal( ReplicatedContent content, WritableChannel channel ) throws IOException
        {
            if ( content instanceof ReplicatedInteger )
            {
                channel.put( (byte) 1 );
                channel.putInt( ((ReplicatedInteger) content).get() );
            }
            else
            {
                throw new IllegalArgumentException( "Unknown content type " + content.getClass() );
            }
        }

        @Override
        public ReplicatedContent unmarshal0( ReadableChannel channel ) throws IOException
        {
            byte type = channel.get();
            final ReplicatedContent content;
            switch ( type )
            {
                case 1:
                    content = ReplicatedInteger.valueOf( channel.getInt() );
                    break;
                default:
                    throw new IllegalArgumentException( String.format( "Unknown content type 0x%x", type ) );
            }
            return content;
        }
    };
}
