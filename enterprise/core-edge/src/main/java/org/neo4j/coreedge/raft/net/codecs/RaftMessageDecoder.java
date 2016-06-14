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
package org.neo4j.coreedge.raft.net.codecs;

import java.io.UnsupportedEncodingException;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.ByteBufMarshal;
import org.neo4j.coreedge.server.CoreMember;

import static org.neo4j.coreedge.raft.RaftMessages.Type.APPEND_ENTRIES_REQUEST;
import static org.neo4j.coreedge.raft.RaftMessages.Type.APPEND_ENTRIES_RESPONSE;
import static org.neo4j.coreedge.raft.RaftMessages.Type.HEARTBEAT;
import static org.neo4j.coreedge.raft.RaftMessages.Type.LOG_COMPACTION_INFO;
import static org.neo4j.coreedge.raft.RaftMessages.Type.NEW_ENTRY_REQUEST;
import static org.neo4j.coreedge.raft.RaftMessages.Type.VOTE_REQUEST;
import static org.neo4j.coreedge.raft.RaftMessages.Type.VOTE_RESPONSE;

public class RaftMessageDecoder extends MessageToMessageDecoder<ByteBuf>
{
    private final ByteBufMarshal<ReplicatedContent> marshal;

    public RaftMessageDecoder( ByteBufMarshal<ReplicatedContent> marshal )
    {
        this.marshal = marshal;
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf buffer, List<Object> list ) throws Exception
    {
        int messageTypeWire = buffer.readInt();

        RaftMessages.Type[] values = RaftMessages.Type.values();
        RaftMessages.Type messageType = values[messageTypeWire];

        CoreMember from = retrieveMember( buffer );

        if ( messageType.equals( VOTE_REQUEST ) )
        {
            CoreMember candidate = retrieveMember( buffer );

            long term = buffer.readLong();
            long lastLogIndex = buffer.readLong();
            long lastLogTerm = buffer.readLong();

            RaftMessages.Vote.Request<CoreMember> request = new RaftMessages.Vote.Request<>(
                    from, term, candidate, lastLogIndex, lastLogTerm );
            list.add( request );
        }
        else if ( messageType.equals( VOTE_RESPONSE ) )
        {
            long term = buffer.readLong();
            boolean voteGranted = buffer.readBoolean();

            RaftMessages.Vote.Response<CoreMember> response = new RaftMessages.Vote.Response<>( from, term,
                    voteGranted );
            list.add( response );
        }
        else if ( messageType.equals( APPEND_ENTRIES_REQUEST ) )
        {
            // how many
            long term = buffer.readLong();
            long prevLogIndex = buffer.readLong();
            long prevLogTerm = buffer.readLong();

            long leaderCommit = buffer.readLong();
            long count = buffer.readLong();

            RaftLogEntry[] entries = new RaftLogEntry[(int) count];
            for ( int i = 0; i < count; i++ )
            {
                long entryTerm = buffer.readLong();
                final ReplicatedContent content = marshal.unmarshal( buffer );
                entries[i] = new RaftLogEntry( entryTerm, content );
            }

            list.add( new RaftMessages.AppendEntries.Request<>( from, term, prevLogIndex, prevLogTerm,
                    entries, leaderCommit ) );
        }
        else if ( messageType.equals( APPEND_ENTRIES_RESPONSE ) )
        {
            long term = buffer.readLong();
            boolean success = buffer.readBoolean();
            long matchIndex = buffer.readLong();
            long appendIndex = buffer.readLong();

            list.add( new RaftMessages.AppendEntries.Response<>( from, term, success, matchIndex, appendIndex ) );
        }
        else if ( messageType.equals( NEW_ENTRY_REQUEST ) )
        {
            ReplicatedContent content = marshal.unmarshal( buffer );

            list.add( new RaftMessages.NewEntry.Request<>( from, content ) );
        }
        else if ( messageType.equals( HEARTBEAT ) )
        {
            long leaderTerm = buffer.readLong();
            long commitIndexTerm = buffer.readLong();
            long commitIndex = buffer.readLong();

            list.add( new RaftMessages.Heartbeat<>( from, leaderTerm, commitIndex, commitIndexTerm ) );
        }
        else if ( messageType.equals( LOG_COMPACTION_INFO ) )
        {
            long leaderTerm = buffer.readLong();
            long prevIndex = buffer.readLong();

            list.add( new RaftMessages.LogCompactionInfo<>( from, leaderTerm, prevIndex ) );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown message type" );
        }
    }

    private CoreMember retrieveMember( ByteBuf buffer ) throws UnsupportedEncodingException
    {
        AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal marshal =
                new AdvertisedSocketAddress.AdvertisedSocketAddressByteBufferMarshal();

        AdvertisedSocketAddress coreAddress = marshal.unmarshal( buffer );
        AdvertisedSocketAddress raftAddress = marshal.unmarshal( buffer );

        return new CoreMember( coreAddress, raftAddress );
    }

    @Override
    public void channelReadComplete( ChannelHandlerContext ctx ) throws Exception
    {
        // TODO: Should we use this?
    }
}
