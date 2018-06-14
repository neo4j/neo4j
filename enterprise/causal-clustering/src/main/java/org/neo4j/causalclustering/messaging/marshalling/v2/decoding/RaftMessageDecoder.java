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
import java.util.Optional;
import java.util.Queue;

import org.neo4j.causalclustering.catchup.Protocol;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
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
        LazyComposer composer;

        if ( messageType.equals( VOTE_REQUEST ) )
        {
            MemberId candidate = retrieveMember( channel );

            long term = channel.getLong();
            long lastLogIndex = channel.getLong();
            long lastLogTerm = channel.getLong();

            composer = new SimpleMessageComposer( new RaftMessages.Vote.Request( from, term, candidate, lastLogIndex, lastLogTerm ) );
        }
        else if ( messageType.equals( VOTE_RESPONSE ) )
        {
            long term = channel.getLong();
            boolean voteGranted = channel.get() == 1;

            composer = new SimpleMessageComposer( new RaftMessages.Vote.Response( from, term, voteGranted ) );
        }
        else if ( messageType.equals( PRE_VOTE_REQUEST ) )
        {
            MemberId candidate = retrieveMember( channel );

            long term = channel.getLong();
            long lastLogIndex = channel.getLong();
            long lastLogTerm = channel.getLong();

            composer = new SimpleMessageComposer( new RaftMessages.PreVote.Request( from, term, candidate, lastLogIndex, lastLogTerm ) );
        }
        else if ( messageType.equals( PRE_VOTE_RESPONSE ) )
        {
            long term = channel.getLong();
            boolean voteGranted = channel.get() == 1;

            composer = new SimpleMessageComposer( new RaftMessages.PreVote.Response( from, term, voteGranted ) );
        }
        else if ( messageType.equals( APPEND_ENTRIES_REQUEST ) )
        {
            // how many
            long term = channel.getLong();
            long prevLogIndex = channel.getLong();
            long prevLogTerm = channel.getLong();
            long leaderCommit = channel.getLong();
            int entryCount = channel.getInt();

            composer = new AppendEntriesComposer( entryCount, from, term, prevLogIndex, prevLogTerm, leaderCommit );
        }
        else if ( messageType.equals( APPEND_ENTRIES_RESPONSE ) )
        {
            long term = channel.getLong();
            boolean success = channel.get() == 1;
            long matchIndex = channel.getLong();
            long appendIndex = channel.getLong();

            composer = new SimpleMessageComposer( new RaftMessages.AppendEntries.Response( from, term, success, matchIndex, appendIndex ) );
        }
        else if ( messageType.equals( NEW_ENTRY_REQUEST ) )
        {
            composer = new NewEntryRequestComposer( from );
        }
        else if ( messageType.equals( HEARTBEAT ) )
        {
            long leaderTerm = channel.getLong();
            long commitIndexTerm = channel.getLong();
            long commitIndex = channel.getLong();

            composer = new SimpleMessageComposer( new RaftMessages.Heartbeat( from, leaderTerm, commitIndex, commitIndexTerm ) );
        }
        else if ( messageType.equals( HEARTBEAT_RESPONSE ) )
        {
            composer = new SimpleMessageComposer( new RaftMessages.HeartbeatResponse( from ) );
        }
        else if ( messageType.equals( LOG_COMPACTION_INFO ) )
        {
            long leaderTerm = channel.getLong();
            long prevIndex = channel.getLong();

            composer = new SimpleMessageComposer( new RaftMessages.LogCompactionInfo( from, leaderTerm, prevIndex ) );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown message type" );
        }

        list.add( new ClusterIdAwareMessageComposer( composer, clusterId ) );
        protocol.expect( ContentType.ContentType );
    }

    static class ClusterIdAwareMessageComposer
    {
        private final LazyComposer composer;
        private final ClusterId clusterId;

        ClusterIdAwareMessageComposer( LazyComposer composer, ClusterId clusterId )
        {
            this.composer = composer;
            this.clusterId = clusterId;
        }

        Optional<RaftMessages.ClusterIdAwareMessage> maybeCompose( Clock clock, Queue<Long> terms, Queue<ReplicatedContent> contents )
        {
            return composer.maybeComplete( terms, contents )
                    .map( m -> ReceivedInstantClusterIdAwareMessage.of( clock.instant(), clusterId, m ) );
        }
    }

    private MemberId retrieveMember( ReadableChannel buffer ) throws IOException, EndOfStreamException
    {
        MemberId.Marshal memberIdMarshal = new MemberId.Marshal();
        return memberIdMarshal.unmarshal( buffer );
    }

    interface LazyComposer
    {
        /**
         * Builds the complete raft message if provided collections contain enough data for building the complete message.
         */
        Optional<RaftMessages.RaftMessage> maybeComplete( Queue<Long> terms, Queue<ReplicatedContent> contents );
    }

    /**
     * A plain message without any more internal content.
     */
    private static class SimpleMessageComposer implements LazyComposer
    {
        private final RaftMessages.RaftMessage message;

        private SimpleMessageComposer( RaftMessages.RaftMessage message )
        {
            this.message = message;
        }

        @Override
        public Optional<RaftMessages.RaftMessage> maybeComplete( Queue<Long> terms, Queue<ReplicatedContent> contents )
        {
            return Optional.of( message );
        }
    }

    private static class AppendEntriesComposer implements LazyComposer
    {
        private final int entryCount;
        private final MemberId from;
        private final long term;
        private final long prevLogIndex;
        private final long prevLogTerm;
        private final long leaderCommit;

        AppendEntriesComposer( int entryCount, MemberId from, long term, long prevLogIndex, long prevLogTerm, long leaderCommit )
        {
            this.entryCount = entryCount;
            this.from = from;
            this.term = term;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.leaderCommit = leaderCommit;
        }

        @Override
        public Optional<RaftMessages.RaftMessage> maybeComplete( Queue<Long> terms, Queue<ReplicatedContent> contents )
        {
            if ( terms.size() < entryCount || contents.size() < entryCount )
            {
                return Optional.empty();
            }

            RaftLogEntry[] entries = new RaftLogEntry[entryCount];
            for ( int i = 0; i < entryCount; i++ )
            {
                long term = terms.remove();
                ReplicatedContent content = contents.remove();
                entries[i] = new RaftLogEntry( term, content );
            }
            return Optional.of( new RaftMessages.AppendEntries.Request( from, term, prevLogIndex, prevLogTerm, entries, leaderCommit ) );
        }
    }

    private static class NewEntryRequestComposer implements LazyComposer
    {
        private final MemberId from;

        NewEntryRequestComposer( MemberId from )
        {
            this.from = from;
        }

        @Override
        public Optional<RaftMessages.RaftMessage> maybeComplete( Queue<Long> terms, Queue<ReplicatedContent> contents )
        {
            if ( contents.isEmpty() )
            {
                return Optional.empty();
            }
            else
            {
                return Optional.of( new RaftMessages.NewEntry.Request( from, contents.remove() ) );
            }
        }
    }
}
