/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.messaging.marshalling.v2.decoding;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Queue;
import java.util.function.BiFunction;

import org.neo4j.causalclustering.catchup.Protocol;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.v2.ContentType;
import org.neo4j.storageengine.api.ReadableChannel;

import static org.neo4j.causalclustering.core.consensus.RaftMessages.Type.APPEND_ENTRIES_REQUEST;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.Type.APPEND_ENTRIES_RESPONSE;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.Type.HEARTBEAT;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.Type.HEARTBEAT_RESPONSE;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.Type.LOG_COMPACTION_INFO;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.Type.NEW_ENTRY_REQUEST;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.Type.PRE_VOTE_REQUEST;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.Type.PRE_VOTE_RESPONSE;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.Type.VOTE_REQUEST;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.Type.VOTE_RESPONSE;

public class RaftMessageDecoder extends ByteToMessageDecoder
{
    private final Protocol<ContentType> protocol;

    RaftMessageDecoder( Protocol<ContentType> protocol )
    {
        this.protocol = protocol;
    }

    @Override
    public void decode( ChannelHandlerContext ctx, ByteBuf buffer, List<Object> list ) throws Exception
    {
        ReadableChannel channel = new NetworkReadableClosableChannelNetty4( buffer );
        ClusterId clusterId = ClusterId.Marshal.INSTANCE.unmarshal( channel );

        int messageTypeWire = channel.getInt();
        RaftMessages.Type[] values = RaftMessages.Type.values();
        RaftMessages.Type messageType = values[messageTypeWire];

        MemberId from = retrieveMember( channel );
        LazyComposer result;

        if ( messageType.equals( VOTE_REQUEST ) )
        {
            MemberId candidate = retrieveMember( channel );

            long term = channel.getLong();
            long lastLogIndex = channel.getLong();
            long lastLogTerm = channel.getLong();

            result = noContent( new RaftMessages.Vote.Request( from, term, candidate, lastLogIndex, lastLogTerm ) );
        }
        else if ( messageType.equals( VOTE_RESPONSE ) )
        {
            long term = channel.getLong();
            boolean voteGranted = channel.get() == 1;

            result = noContent( new RaftMessages.Vote.Response( from, term, voteGranted ) );
        }
        else if ( messageType.equals( PRE_VOTE_REQUEST ) )
        {
            MemberId candidate = retrieveMember( channel );

            long term = channel.getLong();
            long lastLogIndex = channel.getLong();
            long lastLogTerm = channel.getLong();

            result = noContent( new RaftMessages.PreVote.Request( from, term, candidate, lastLogIndex, lastLogTerm ) );
        }
        else if ( messageType.equals( PRE_VOTE_RESPONSE ) )
        {
            long term = channel.getLong();
            boolean voteGranted = channel.get() == 1;

            result = noContent( new RaftMessages.PreVote.Response( from, term, voteGranted ) );
        }
        else if ( messageType.equals( APPEND_ENTRIES_REQUEST ) )
        {
            // how many
            long term = channel.getLong();
            long prevLogIndex = channel.getLong();
            long prevLogTerm = channel.getLong();
            long leaderCommit = channel.getLong();
            int raftLogEntries = channel.getInt();

            result = new AppendEntriesComposer( raftLogEntries, from, term, prevLogIndex, prevLogTerm, leaderCommit );
        }
        else if ( messageType.equals( APPEND_ENTRIES_RESPONSE ) )
        {
            long term = channel.getLong();
            boolean success = channel.get() == 1;
            long matchIndex = channel.getLong();
            long appendIndex = channel.getLong();

            result = noContent( new RaftMessages.AppendEntries.Response( from, term, success, matchIndex, appendIndex ) );
        }
        else if ( messageType.equals( NEW_ENTRY_REQUEST ) )
        {
            result = new NewEntryRequestComposer( from );
        }
        else if ( messageType.equals( HEARTBEAT ) )
        {
            long leaderTerm = channel.getLong();
            long commitIndexTerm = channel.getLong();
            long commitIndex = channel.getLong();

            result = noContent( new RaftMessages.Heartbeat( from, leaderTerm, commitIndex, commitIndexTerm ) );
        }
        else if ( messageType.equals( HEARTBEAT_RESPONSE ) )
        {
            result = noContent( new RaftMessages.HeartbeatResponse( from ) );
        }
        else if ( messageType.equals( LOG_COMPACTION_INFO ) )
        {
            long leaderTerm = channel.getLong();
            long prevIndex = channel.getLong();

            result = noContent( new RaftMessages.LogCompactionInfo( from, leaderTerm, prevIndex ) );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown message type" );
        }

        list.add( new ClusterIdAwareMessageComposer( result, clusterId ) );
        protocol.expect( ContentType.ContentType );
    }

    static class ClusterIdAwareMessageComposer
    {
        private final LazyComposer result;
        private final ClusterId clusterId;

        ClusterIdAwareMessageComposer( LazyComposer result, ClusterId clusterId )
        {
            this.result = result;
            this.clusterId = clusterId;
        }

        RaftMessages.ClusterIdAwareMessage maybeCompose( Clock clock, Queue<Long> logEntryTerms, Queue<ReplicatedContent> replicatedContents )
        {
            RaftMessages.RaftMessage apply = result.apply( logEntryTerms, replicatedContents );
            if ( apply != null )
            {
                return RaftMessages.ReceivedInstantClusterIdAwareMessage.of( clock.instant(), clusterId, apply );
            }
            else
            {
                return null;
            }
        }
    }

    private MemberId retrieveMember( ReadableChannel buffer ) throws IOException, EndOfStreamException
    {
        MemberId.Marshal memberIdMarshal = new MemberId.Marshal();
        return memberIdMarshal.unmarshal( buffer );
    }

    /**
     * Builds the raft message. Should return {@code null} if provided collections does not contain enough data for building the message.
     */
    interface LazyComposer extends BiFunction<Queue<Long>,Queue<ReplicatedContent>,RaftMessages.RaftMessage> {}

    private static LazyComposer noContent( RaftMessages.RaftMessage message )
    {
        return ( rle, rc ) -> message;
    }

    private static class AppendEntriesComposer implements LazyComposer
    {
        private final int raftLogEntries;
        private final MemberId from;
        private final long term;
        private final long prevLogIndex;
        private final long prevLogTerm;
        private final long leaderCommit;

        AppendEntriesComposer( int raftLogEntries, MemberId from, long term, long prevLogIndex, long prevLogTerm, long leaderCommit )
        {
            this.raftLogEntries = raftLogEntries;
            this.from = from;
            this.term = term;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.leaderCommit = leaderCommit;
        }

        @Override
        public RaftMessages.BaseRaftMessage apply( Queue<Long> rle, Queue<ReplicatedContent> rc )
        {
            if ( rle.size() < raftLogEntries || rc.size() < raftLogEntries )
            {
                return null;
            }
            else
            {
                RaftLogEntry[] entries = new RaftLogEntry[raftLogEntries];
                for ( int i = 0; i < raftLogEntries; i++ )
                {
                    Long poll = rle.poll();
                    if ( poll == null )
                    {
                        throw new IllegalArgumentException( "Term cannot be null" );
                    }
                    entries[i] = new RaftLogEntry( poll, rc.poll() );
                }
                return new RaftMessages.AppendEntries.Request( from, term, prevLogIndex, prevLogTerm, entries, leaderCommit );
            }
        }
    }

    private static class NewEntryRequestComposer implements LazyComposer
    {
        private final MemberId from;

        NewEntryRequestComposer( MemberId from ) {this.from = from;}

        @Override
        public RaftMessages.BaseRaftMessage apply( Queue<Long> rle, Queue<ReplicatedContent> rc )
        {
            if ( rc.isEmpty() )
            {
                return null;
            }
            else
            {
                return new RaftMessages.NewEntry.Request( from, rc.poll() );
            }
        }
    }
}
