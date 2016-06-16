/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.net;

import java.io.IOException;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.net.codecs.RaftMessageDecoder;
import org.neo4j.coreedge.raft.net.codecs.RaftMessageEncoder;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.kernel.impl.store.StoreId;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class RaftMessageProcessingTest
{
    private static ChannelMarshal<ReplicatedContent> serializer = new ChannelMarshal<ReplicatedContent>()
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
        public ReplicatedContent unmarshal( ReadableChannel channel ) throws IOException
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

            try
            {
                channel.get();
                throw new IllegalArgumentException( "Bytes remain in buffer after deserialization" );
            }
            catch ( ReadPastEndException e )
            {
                // expected
            }
            return content;
        }
    };

    private EmbeddedChannel channel;
    private StoreId storeId = new StoreId( 1, 2, 3, 4, 5 );

    @Before
    public void setup()
    {
        channel = new EmbeddedChannel( new RaftMessageEncoder( serializer ), new RaftMessageDecoder( serializer ) );
    }

    @Test
    public void shouldEncodeAndDecodeVoteRequest()
    {
        // given
        CoreMember member = new CoreMember( new AdvertisedSocketAddress( "host1:9000" ),
                new AdvertisedSocketAddress( "host1:9001" ) );
        RaftMessages.Vote.Request request = new RaftMessages.Vote.Request<>( member, 1, member, 1, 1, storeId );

        // when
        channel.writeOutbound( request );
        channel.writeInbound( channel.readOutbound() );

        // then
        assertEquals( request, channel.readInbound() );
    }

    @Test
    public void shouldEncodeAndDecodeVoteResponse()
    {
        // given
        CoreMember member = new CoreMember( new AdvertisedSocketAddress( "host1:9000" ),
                new AdvertisedSocketAddress( "host1:9001" ) );
        RaftMessages.Vote.Response response = new RaftMessages.Vote.Response<>( member, 1, true, storeId );

        // when
        channel.writeOutbound( response );
        channel.writeInbound( channel.readOutbound() );

        // then
        assertEquals( response, channel.readInbound() );
    }

    @Test
    public void shouldEncodeAndDecodeAppendEntriesRequest()
    {
        // given
        CoreMember member = new CoreMember( new AdvertisedSocketAddress( "host1:9000" ),
                new AdvertisedSocketAddress( "host1:9001" ) );
        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        RaftMessages.AppendEntries.Request request =
                new RaftMessages.AppendEntries.Request<>( member, 1, 1, 99, new RaftLogEntry[]{ logEntry }, 1, storeId );

        // when
        channel.writeOutbound( request );
        channel.writeInbound( channel.readOutbound() );

        // then
        assertEquals( request, channel.readInbound() );
    }

    @Test
    public void shouldEncodeAndDecodeAppendEntriesResponse()
    {
        // given
        CoreMember member = new CoreMember( new AdvertisedSocketAddress( "host1:9000" ),
                new AdvertisedSocketAddress( "host1:9001" ) );
        RaftMessages.AppendEntries.Response response =
                new RaftMessages.AppendEntries.Response<>( member, 1, false, -1, 0, storeId );

        // when
        channel.writeOutbound( response );
        channel.writeInbound( channel.readOutbound() );

        // then
        assertEquals( response, channel.readInbound() );
    }

    @Test
    public void shouldEncodeAndDecodeNewEntryRequest()
    {
        // given
        CoreMember member = new CoreMember( new AdvertisedSocketAddress( "host1:9000" ),
                new AdvertisedSocketAddress( "host1:9001" ) );
        RaftMessages.NewEntry.Request request =
                new RaftMessages.NewEntry.Request<>( member, ReplicatedInteger.valueOf( 12 ), storeId );

        // when
        channel.writeOutbound( request );
        channel.writeInbound( channel.readOutbound() );

        // then
        assertEquals( request, channel.readInbound() );
    }
}
