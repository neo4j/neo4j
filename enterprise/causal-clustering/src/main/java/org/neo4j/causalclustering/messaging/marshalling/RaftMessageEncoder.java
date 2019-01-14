/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.NetworkFlushableByteBuf;

public class RaftMessageEncoder extends MessageToByteEncoder<RaftMessages.ClusterIdAwareMessage>
{
    private final ChannelMarshal<ReplicatedContent> marshal;

    public RaftMessageEncoder( ChannelMarshal<ReplicatedContent> marshal )
    {
        this.marshal = marshal;
    }

    @Override
    protected synchronized void encode( ChannelHandlerContext ctx,
            RaftMessages.ClusterIdAwareMessage decoratedMessage,
            ByteBuf out ) throws Exception
    {
        RaftMessages.RaftMessage message = decoratedMessage.message();
        ClusterId clusterId = decoratedMessage.clusterId();
        MemberId.Marshal memberMarshal = new MemberId.Marshal();

        NetworkFlushableByteBuf channel = new NetworkFlushableByteBuf( out );
        ClusterId.Marshal.INSTANCE.marshal( clusterId, channel );
        channel.putInt( message.type().ordinal() );
        memberMarshal.marshal( message.from(), channel );

        message.dispatch( new Handler( marshal, memberMarshal, channel ) );
    }

    private static class Handler implements RaftMessages.Handler<Void, Exception>
    {
        private final ChannelMarshal<ReplicatedContent> marshal;
        private final MemberId.Marshal memberMarshal;
        private final NetworkFlushableByteBuf channel;

        Handler( ChannelMarshal<ReplicatedContent> marshal, MemberId.Marshal memberMarshal, NetworkFlushableByteBuf channel )
        {
            this.marshal = marshal;
            this.memberMarshal = memberMarshal;
            this.channel = channel;
        }

        @Override
        public Void handle( RaftMessages.Vote.Request voteRequest ) throws Exception
        {
            memberMarshal.marshal( voteRequest.candidate(), channel );
            channel.putLong( voteRequest.term() );
            channel.putLong( voteRequest.lastLogIndex() );
            channel.putLong( voteRequest.lastLogTerm() );

            return null;
        }

        @Override
        public Void handle( RaftMessages.Vote.Response voteResponse )
        {
            channel.putLong( voteResponse.term() );
            channel.put( (byte) (voteResponse.voteGranted() ? 1 : 0) );

            return null;
        }

        @Override
        public Void handle( RaftMessages.PreVote.Request preVoteRequest ) throws Exception
        {
            memberMarshal.marshal( preVoteRequest.candidate(), channel );
            channel.putLong( preVoteRequest.term() );
            channel.putLong( preVoteRequest.lastLogIndex() );
            channel.putLong( preVoteRequest.lastLogTerm() );

            return null;
        }

        @Override
        public Void handle( RaftMessages.PreVote.Response preVoteResponse )
        {
            channel.putLong( preVoteResponse.term() );
            channel.put( (byte) (preVoteResponse.voteGranted() ? 1 : 0) );

            return null;
        }

        @Override
        public Void handle( RaftMessages.AppendEntries.Request appendRequest ) throws Exception
        {
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

            return null;
        }

        @Override
        public Void handle( RaftMessages.AppendEntries.Response appendResponse )
        {
            channel.putLong( appendResponse.term() );
            channel.put( (byte) (appendResponse.success() ? 1 : 0) );
            channel.putLong( appendResponse.matchIndex() );
            channel.putLong( appendResponse.appendIndex() );

            return null;
        }

        @Override
        public Void handle( RaftMessages.NewEntry.Request newEntryRequest ) throws Exception
        {
            marshal.marshal( newEntryRequest.content(), channel );

            return null;
        }

        @Override
        public Void handle( RaftMessages.Heartbeat heartbeat )
        {
            channel.putLong( heartbeat.leaderTerm() );
            channel.putLong( heartbeat.commitIndexTerm() );
            channel.putLong( heartbeat.commitIndex() );

            return null;
        }

        @Override
        public Void handle( RaftMessages.HeartbeatResponse heartbeatResponse )
        {
            // Heartbeat Response does not have any data attached to it.
            return null;
        }

        @Override
        public Void handle( RaftMessages.LogCompactionInfo logCompactionInfo )
        {
            channel.putLong( logCompactionInfo.leaderTerm() );
            channel.putLong( logCompactionInfo.prevIndex() );
            return null;
        }

        @Override
        public Void handle( RaftMessages.Timeout.Election election )
        {
            return null; // Not network
        }

        @Override
        public Void handle( RaftMessages.Timeout.Heartbeat heartbeat )
        {
            return null; // Not network
        }

        @Override
        public Void handle( RaftMessages.NewEntry.BatchRequest batchRequest )
        {
            return null; // Not network
        }

        @Override
        public Void handle( RaftMessages.PruneRequest pruneRequest )
        {
            return null; // Not network
        }
    }
}
