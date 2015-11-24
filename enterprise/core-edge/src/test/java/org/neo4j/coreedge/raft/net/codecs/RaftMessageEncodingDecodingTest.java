/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.net.codecs;

import java.util.ArrayList;
import java.util.LinkedList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import org.neo4j.coreedge.raft.AppendEntriesRequestBuilder;
import org.neo4j.coreedge.raft.AppendEntriesResponseBuilder;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.VoteRequestBuilder;
import org.neo4j.coreedge.raft.VoteResponseBuilder;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.replication.MarshallingException;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.ReplicatedContentMarshal;
import org.neo4j.coreedge.server.CoreMember;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public class RaftMessageEncodingDecodingTest
{
    @Test
    public void shouldSerializeAppendRequestWithMultipleEntries() throws Exception
    {
        CoreMember sender = new CoreMember( address( "127.0.0.1:5001" ), address( "127.0.0.2:5001" ) );
        RaftMessages.AppendEntries.Request<CoreMember> request = new AppendEntriesRequestBuilder<CoreMember>()
                .from( sender )
                .leader( sender )
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
        CoreMember sender = new CoreMember( address( "127.0.0.1:5001" ), address( "127.0.0.2:5001" ) );
        RaftMessages.AppendEntries.Request<CoreMember> request = new AppendEntriesRequestBuilder<CoreMember>()
                .from( sender )
                .leader( sender )
                .leaderCommit( 2 )
                .leaderTerm( 4 )
                .build();
        serializeReadBackAndVerifyMessage( request );
    }

    @Test
    public void shouldSerializeAppendResponse() throws Exception
    {
        CoreMember sender = new CoreMember( address( "127.0.0.1:5001" ), address( "127.0.0.2:5001" ) );
        RaftMessages.AppendEntries.Response<CoreMember> request = new AppendEntriesResponseBuilder<CoreMember>()
                .from( sender )
                .success()
                .matchIndex( 12 )
                .build();
        serializeReadBackAndVerifyMessage( request );
    }

    @Test
    public void shouldSerializeHeartbeats(  ) throws Exception
    {
        // Given
        RaftMessageEncoder encoder = new RaftMessageEncoder( serializer );
        RaftMessageDecoder decoder = new RaftMessageDecoder( serializer );

        // Netty puts buffers with serialized content in this list
        LinkedList<Object> resultingBuffers = new LinkedList<>();
        // Deserialization adds read objects in this list
        ArrayList<Object> thingsRead = new ArrayList<>( 1 );

        // When
        CoreMember sender = new CoreMember( address( "127.0.0.1:5001" ), address( "127.0.0.2:5001" ) );
        RaftMessages.Heartbeat<CoreMember> message = new RaftMessages.Heartbeat<>( sender, 1, 2, 3 );
        encoder.encode( setupContext(), message, resultingBuffers );

        // Then
        assertEquals( 1, resultingBuffers.size() );

        // When
        decoder.decode( null, (ByteBuf) resultingBuffers.get( 0 ), thingsRead );

        // Then
        assertEquals( 1, thingsRead.size() );
        assertEquals( message, thingsRead.get( 0 ) );
    }

    @Test
    public void shouldSerializeVoteRequest() throws Exception
    {
        CoreMember sender = new CoreMember( address( "127.0.0.1:5001" ), address( "127.0.0.2:5001" ) );
        RaftMessages.Vote.Request<Object> request = new VoteRequestBuilder<>()
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
        CoreMember sender = new CoreMember( address( "127.0.0.1:5001" ), address( "127.0.0.2:5001" ) );
        RaftMessages.Vote.Response<Object> request = new VoteResponseBuilder<>()
                .from( sender )
                .grant()
                .term( 3 )
                .build();
        serializeReadBackAndVerifyMessage( request );
    }

    public void serializeReadBackAndVerifyMessage( RaftMessages.Message message ) throws Exception
    {
        // Given
        RaftMessageEncoder encoder = new RaftMessageEncoder( serializer );
        RaftMessageDecoder decoder = new RaftMessageDecoder( serializer );

        // Netty puts buffers with serialized content in this list
        LinkedList<Object> resultingBuffers = new LinkedList<>();
        // Deserialization adds read objects in this list
        ArrayList<Object> thingsRead = new ArrayList<>( 1 );

        // When
        encoder.encode( setupContext(), message, resultingBuffers );

        // Then
        assertEquals( 1, resultingBuffers.size() );

        // When
        decoder.decode( null, (ByteBuf) resultingBuffers.get( 0 ), thingsRead );

        // Then
        assertEquals( 1, thingsRead.size() );
        assertEquals( message, thingsRead.get( 0 ) );
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
    private static final ReplicatedContentMarshal<ByteBuf> serializer = new ReplicatedContentMarshal<ByteBuf>()
    {
        @Override
        public void serialize( ReplicatedContent content, ByteBuf buffer ) throws MarshallingException
        {
            if ( content instanceof ReplicatedInteger )
            {
                buffer.writeByte( 1 );
                buffer.writeInt( ((ReplicatedInteger) content).get() );
            }
            else
            {
                throw new IllegalArgumentException( "Unknown content type " + content.getClass() );
            }
        }

        @Override
        public ReplicatedContent deserialize( ByteBuf buffer ) throws MarshallingException
        {
            if ( buffer.readableBytes() < 1 )
            {
                throw new MarshallingException( "Cannot read content type" );
            }
            byte type = buffer.readByte();
            final ReplicatedContent content;
            switch ( type )
            {
                case 1:
                    content = ReplicatedInteger.valueOf( buffer.readInt() );
                    break;
                default:
                    throw new MarshallingException( String.format( "Unknown content type 0x%x", type ) );
            }
            return content;
        }
    };
}
