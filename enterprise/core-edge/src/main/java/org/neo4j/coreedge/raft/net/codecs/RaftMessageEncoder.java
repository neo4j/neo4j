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

import java.io.UnsupportedEncodingException;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.AdvertisedSocketAddressEncoder;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.replication.ReplicatedContentMarshal;

public class RaftMessageEncoder extends MessageToMessageEncoder<RaftMessages.Message<CoreMember>>
{
    private final ReplicatedContentMarshal<ByteBuf> marshal;

    public RaftMessageEncoder( ReplicatedContentMarshal<ByteBuf> marshal )
    {
        this.marshal = marshal;
    }

    @Override
    protected synchronized void encode( ChannelHandlerContext ctx, RaftMessages.Message<CoreMember> message,
                                        List<Object> list ) throws Exception
    {
        ByteBuf buf = ctx.alloc().buffer();

        buf.writeInt( message.type().ordinal() );
        writeMember( message.from(), buf );

        if ( message instanceof RaftMessages.Vote.Request )
        {
            RaftMessages.Vote.Request<CoreMember> voteRequest = (RaftMessages.Vote.Request<CoreMember>) message;
            writeMember( voteRequest.candidate(), buf );
            buf.writeLong( voteRequest.term() );
            buf.writeLong( voteRequest.lastLogIndex() );
            buf.writeLong( voteRequest.lastLogTerm() );
        }
        else if ( message instanceof RaftMessages.Vote.Response )
        {
            RaftMessages.Vote.Response<CoreMember> voteResponse = (RaftMessages.Vote.Response<CoreMember>) message;
            buf.writeLong( voteResponse.term() );
            buf.writeBoolean( voteResponse.voteGranted() );
        }
        else if ( message instanceof RaftMessages.AppendEntries.Request )
        {
            RaftMessages.AppendEntries.Request<CoreMember> appendRequest = (RaftMessages.AppendEntries
                    .Request<CoreMember>) message;

            buf.writeLong( appendRequest.leaderTerm() );
            buf.writeLong( appendRequest.prevLogIndex() );
            buf.writeLong( appendRequest.prevLogTerm() );
            buf.writeLong( appendRequest.leaderCommit() );

            buf.writeLong( appendRequest.entries().length );

            for ( RaftLogEntry raftLogEntry : appendRequest.entries() )
            {
                buf.writeLong( raftLogEntry.term() );
                marshal.serialize( raftLogEntry.content(), buf );
            }
        }
        else if ( message instanceof RaftMessages.AppendEntries.Response )
        {
            RaftMessages.AppendEntries.Response<CoreMember> appendResponse = (RaftMessages.AppendEntries
                    .Response<CoreMember>) message;

            buf.writeLong( appendResponse.term() );
            buf.writeBoolean( appendResponse.success() );
            buf.writeLong( appendResponse.matchIndex() );
            buf.writeLong( appendResponse.appendIndex() );
        }
        else if ( message instanceof RaftMessages.NewEntry.Request )
        {
            RaftMessages.NewEntry.Request<CoreMember> newEntryRequest = (RaftMessages.NewEntry
                    .Request<CoreMember>) message;
            marshal.serialize( newEntryRequest.content(), buf );
        }
        else if ( message instanceof RaftMessages.Heartbeat )
        {
            RaftMessages.Heartbeat<CoreMember> heartbeat = (RaftMessages.Heartbeat<CoreMember>) message;
            buf.writeLong( heartbeat.leaderTerm() );
            buf.writeLong( heartbeat.commitIndexTerm() );
            buf.writeLong( heartbeat.commitIndex() );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown message type" );
        }

        list.add( buf );
    }

    private void writeMember( CoreMember member, ByteBuf buffer ) throws UnsupportedEncodingException
    {
        AdvertisedSocketAddressEncoder encoder = new AdvertisedSocketAddressEncoder();
        encoder.encode( member.getCoreAddress(), buffer );
        encoder.encode( member.getRaftAddress(), buffer );
    }
}
