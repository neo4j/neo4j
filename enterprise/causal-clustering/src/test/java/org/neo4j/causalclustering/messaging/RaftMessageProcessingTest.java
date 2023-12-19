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
package org.neo4j.causalclustering.messaging;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Clock;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageDecoder;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageEncoder;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.junit.Assert.assertEquals;

@RunWith( MockitoJUnitRunner.class )
public class RaftMessageProcessingTest
{
    private static ChannelMarshal<ReplicatedContent> serializer = new SafeChannelMarshal<ReplicatedContent>()
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

    @Before
    public void setup()
    {
        channel = new EmbeddedChannel( new RaftMessageEncoder( serializer ), new RaftMessageDecoder( serializer, Clock.systemUTC() ) );
    }

    @Test
    public void shouldEncodeAndDecodeVoteRequest()
    {
        // given
        MemberId member = new MemberId( UUID.randomUUID() );
        RaftMessages.Vote.Request request = new RaftMessages.Vote.Request( member, 1, member, 1, 1 );

        // when
        channel.writeOutbound( request );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        assertEquals( request, channel.readInbound() );
    }

    @Test
    public void shouldEncodeAndDecodeVoteResponse()
    {
        // given
        MemberId member = new MemberId( UUID.randomUUID() );
        RaftMessages.Vote.Response response = new RaftMessages.Vote.Response( member, 1, true );

        // when
        channel.writeOutbound( response );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        assertEquals( response, channel.readInbound() );
    }

    @Test
    public void shouldEncodeAndDecodeAppendEntriesRequest()
    {
        // given
        MemberId member = new MemberId( UUID.randomUUID() );
        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        RaftMessages.AppendEntries.Request request =
                new RaftMessages.AppendEntries.Request(
                        member, 1, 1, 99, new RaftLogEntry[] { logEntry }, 1 );

        // when
        channel.writeOutbound( request );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        assertEquals( request, channel.readInbound() );
    }

    @Test
    public void shouldEncodeAndDecodeAppendEntriesResponse()
    {
        // given
        MemberId member = new MemberId( UUID.randomUUID() );
        RaftMessages.AppendEntries.Response response =
                new RaftMessages.AppendEntries.Response( member, 1, false, -1, 0 );

        // when
        channel.writeOutbound( response );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        assertEquals( response, channel.readInbound() );
    }

    @Test
    public void shouldEncodeAndDecodeNewEntryRequest()
    {
        // given
        MemberId member = new MemberId( UUID.randomUUID() );
        RaftMessages.NewEntry.Request request =
                new RaftMessages.NewEntry.Request( member, ReplicatedInteger.valueOf( 12 ) );

        // when
        channel.writeOutbound( request );
        Object message = channel.readOutbound();
        channel.writeInbound( message );

        // then
        assertEquals( request, channel.readInbound() );
    }
}
