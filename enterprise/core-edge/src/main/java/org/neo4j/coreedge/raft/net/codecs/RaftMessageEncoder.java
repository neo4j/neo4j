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

import java.io.IOException;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import org.neo4j.coreedge.catchup.storecopy.core.NetworkFlushableByteBuf;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.coreedge.raft.replication.storeid.StoreIdMarshal;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.storageengine.api.WritableChannel;

public class RaftMessageEncoder extends MessageToMessageEncoder<RaftMessages.RaftMessage<CoreMember>>
{
    private final ChannelMarshal<ReplicatedContent> marshal;

    public RaftMessageEncoder( ChannelMarshal<ReplicatedContent> marshal )
    {
        this.marshal = marshal;
    }

    @Override
    protected synchronized void encode( ChannelHandlerContext ctx, RaftMessages.RaftMessage<CoreMember> message,
                                        List<Object> list ) throws Exception
    {
        ByteBuf buffer = ctx.alloc().buffer();
        WritableChannel channel = new NetworkFlushableByteBuf( buffer );

        channel.putInt( message.type().ordinal() );
        writeMember( message.from(), channel );
        StoreIdMarshal.marshal( message.storeId(), channel );

        if ( message instanceof RaftMessages.Vote.Request )
        {
            RaftMessages.Vote.Request<CoreMember> voteRequest = (RaftMessages.Vote.Request<CoreMember>) message;
            writeMember( voteRequest.candidate(), channel );
            channel.putLong( voteRequest.term() );
            channel.putLong( voteRequest.lastLogIndex() );
            channel.putLong( voteRequest.lastLogTerm() );
        }
        else if ( message instanceof RaftMessages.Vote.Response )
        {
            RaftMessages.Vote.Response<CoreMember> voteResponse = (RaftMessages.Vote.Response<CoreMember>) message;
            channel.putLong( voteResponse.term() );
            channel.put( (byte) (voteResponse.voteGranted() ? 1 : 0) );
        }
        else if ( message instanceof RaftMessages.AppendEntries.Request )
        {
            RaftMessages.AppendEntries.Request<CoreMember> appendRequest = (RaftMessages.AppendEntries
                    .Request<CoreMember>) message;

            channel.putLong( appendRequest.leaderTerm() );
            channel.putLong( appendRequest.prevLogIndex() );
            channel.putLong( appendRequest.prevLogTerm() );
            channel.putLong( appendRequest.leaderCommit() );

            channel.putLong( appendRequest.entries().length );

            for ( RaftLogEntry raftLogEntry : appendRequest.entries() )
            {
                channel.putLong( raftLogEntry.term() );
                marshal.marshal( raftLogEntry.content(), channel );
            }
        }
        else if ( message instanceof RaftMessages.AppendEntries.Response )
        {
            RaftMessages.AppendEntries.Response<CoreMember> appendResponse =
                    (RaftMessages.AppendEntries.Response<CoreMember>) message;

            channel.putLong( appendResponse.term() );
            channel.put( (byte) (appendResponse.success() ? 1 : 0) );
            channel.putLong( appendResponse.matchIndex() );
            channel.putLong( appendResponse.appendIndex() );
        }
        else if ( message instanceof RaftMessages.NewEntry.Request )
        {
            RaftMessages.NewEntry.Request<CoreMember> newEntryRequest = (RaftMessages.NewEntry
                    .Request<CoreMember>) message;
            marshal.marshal( newEntryRequest.content(), channel );
        }
        else if ( message instanceof RaftMessages.Heartbeat )
        {
            RaftMessages.Heartbeat<CoreMember> heartbeat = (RaftMessages.Heartbeat<CoreMember>) message;
            channel.putLong( heartbeat.leaderTerm() );
            channel.putLong( heartbeat.commitIndexTerm() );
            channel.putLong( heartbeat.commitIndex() );
        }
        else if( message instanceof RaftMessages.LogCompactionInfo )
        {
            RaftMessages.LogCompactionInfo<CoreMember> logCompactionInfo = (RaftMessages.LogCompactionInfo<CoreMember>) message;
            channel.putLong( logCompactionInfo.leaderTerm() );
            channel.putLong( logCompactionInfo.prevIndex() );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown message type" );
        }

        list.add( buffer );
    }

    private void writeMember( CoreMember member, WritableChannel buffer ) throws IOException
    {
        AdvertisedSocketAddress.AdvertisedSocketAddressChannelMarshal marshal =
                new AdvertisedSocketAddress.AdvertisedSocketAddressChannelMarshal();

        marshal.marshal( member.getCoreAddress(), buffer );
        marshal.marshal( member.getRaftAddress(), buffer );
    }
}
