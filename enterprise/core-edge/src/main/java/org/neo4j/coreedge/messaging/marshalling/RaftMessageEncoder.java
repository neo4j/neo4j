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
package org.neo4j.coreedge.messaging.marshalling;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.log.RaftLogEntry;
import org.neo4j.coreedge.core.replication.ReplicatedContent;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.coreedge.messaging.NetworkFlushableByteBuf;
import org.neo4j.coreedge.messaging.marshalling.storeid.StoreIdMarshal;

public class RaftMessageEncoder extends MessageToMessageEncoder<RaftMessages.StoreIdAwareMessage>
{
    private final ChannelMarshal<ReplicatedContent> marshal;

    public RaftMessageEncoder( ChannelMarshal<ReplicatedContent> marshal )
    {
        this.marshal = marshal;
    }

    @Override
    protected synchronized void encode( ChannelHandlerContext ctx,
            RaftMessages.StoreIdAwareMessage decoratedMessage,
            List<Object> list ) throws Exception
    {
        RaftMessages.RaftMessage message = decoratedMessage.message();
        StoreId storeId = decoratedMessage.storeId();
        MemberId.Marshal memberMarshal = new MemberId.Marshal();

        NetworkFlushableByteBuf channel = new NetworkFlushableByteBuf( ctx.alloc().buffer() );
        StoreIdMarshal.marshal( storeId, channel );
        channel.putInt( message.type().ordinal() );
        memberMarshal.marshal( message.from(), channel );

        if ( message instanceof RaftMessages.Vote.Request )
        {
            RaftMessages.Vote.Request voteRequest = (RaftMessages.Vote.Request) message;
            memberMarshal.marshal( voteRequest.candidate(), channel );
            channel.putLong( voteRequest.term() );
            channel.putLong( voteRequest.lastLogIndex() );
            channel.putLong( voteRequest.lastLogTerm() );
        }
        else if ( message instanceof RaftMessages.Vote.Response )
        {
            RaftMessages.Vote.Response voteResponse = (RaftMessages.Vote.Response) message;
            channel.putLong( voteResponse.term() );
            channel.put( (byte) (voteResponse.voteGranted() ? 1 : 0) );
        }
        else if ( message instanceof RaftMessages.AppendEntries.Request )
        {
            RaftMessages.AppendEntries.Request appendRequest = (RaftMessages.AppendEntries
                    .Request) message;

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
            RaftMessages.AppendEntries.Response appendResponse =
                    (RaftMessages.AppendEntries.Response) message;

            channel.putLong( appendResponse.term() );
            channel.put( (byte) (appendResponse.success() ? 1 : 0) );
            channel.putLong( appendResponse.matchIndex() );
            channel.putLong( appendResponse.appendIndex() );
        }
        else if ( message instanceof RaftMessages.NewEntry.Request )
        {
            RaftMessages.NewEntry.Request newEntryRequest = (RaftMessages.NewEntry
                    .Request) message;
            marshal.marshal( newEntryRequest.content(), channel );
        }
        else if ( message instanceof RaftMessages.Heartbeat )
        {
            RaftMessages.Heartbeat heartbeat = (RaftMessages.Heartbeat) message;
            channel.putLong( heartbeat.leaderTerm() );
            channel.putLong( heartbeat.commitIndexTerm() );
            channel.putLong( heartbeat.commitIndex() );
        }
        else if ( message instanceof RaftMessages.LogCompactionInfo )
        {
            RaftMessages.LogCompactionInfo logCompactionInfo = (RaftMessages.LogCompactionInfo) message;
            channel.putLong( logCompactionInfo.leaderTerm() );
            channel.putLong( logCompactionInfo.prevIndex() );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown message type" );
        }

        list.add( channel.buffer() );
    }
}
