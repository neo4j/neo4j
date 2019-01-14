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
package org.neo4j.causalclustering.core.consensus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Message;

import static java.lang.String.format;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.Type.HEARTBEAT_RESPONSE;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.Type.PRUNE_REQUEST;

public interface RaftMessages
{

    interface Handler<T, E extends Exception>
    {
        T handle( Vote.Request request ) throws E;
        T handle( Vote.Response response ) throws E;
        T handle( PreVote.Request request ) throws E;
        T handle( PreVote.Response response ) throws E;
        T handle( AppendEntries.Request request ) throws E;
        T handle( AppendEntries.Response response ) throws E;
        T handle( Heartbeat heartbeat ) throws E;
        T handle( LogCompactionInfo logCompactionInfo ) throws E;
        T handle( HeartbeatResponse heartbeatResponse ) throws E;
        T handle( Timeout.Election election ) throws E;
        T handle( Timeout.Heartbeat heartbeat ) throws E;
        T handle( NewEntry.Request request ) throws E;
        T handle( NewEntry.BatchRequest batchRequest ) throws E;
        T handle( PruneRequest pruneRequest ) throws E;
    }

    // Position is used to identify messages. Changing order will break upgrade paths.
    enum Type
    {
        VOTE_REQUEST,
        VOTE_RESPONSE,

        APPEND_ENTRIES_REQUEST,
        APPEND_ENTRIES_RESPONSE,

        HEARTBEAT,
        HEARTBEAT_RESPONSE,
        LOG_COMPACTION_INFO,

        // Timeouts
        ELECTION_TIMEOUT,
        HEARTBEAT_TIMEOUT,

        // TODO: Refactor, these are client-facing messages / api. Perhaps not public and instantiated through an api
        // TODO: method instead?
        NEW_ENTRY_REQUEST,
        NEW_BATCH_REQUEST,

        PRUNE_REQUEST,

        PRE_VOTE_REQUEST,
        PRE_VOTE_RESPONSE,
    }

    interface RaftMessage extends Message
    {
        MemberId from();
        Type type();
        <T, E extends Exception> T dispatch( Handler<T, E> handler ) throws E;
    }

    class Directed
    {
        MemberId to;
        RaftMessage message;

        public Directed( MemberId to, RaftMessage message )
        {
            this.to = to;
            this.message = message;
        }

        public MemberId to()
        {
            return to;
        }

        public RaftMessage message()
        {
            return message;
        }

        @Override
        public String toString()
        {
            return format( "Directed{to=%s, message=%s}", to, message );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Directed directed = (Directed) o;
            return Objects.equals( to, directed.to ) && Objects.equals( message, directed.message );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( to, message );
        }
    }

    interface AnyVote
    {
        interface Request
        {
            long term();

            long lastLogTerm();

            long lastLogIndex();

            MemberId candidate();
        }

        interface Response
        {
            long term();

            boolean voteGranted();
        }
    }

    interface Vote
    {
        class Request extends BaseRaftMessage implements AnyVote.Request
        {
            private long term;
            private MemberId candidate;
            private long lastLogIndex;
            private long lastLogTerm;

            public Request( MemberId from, long term, MemberId candidate, long lastLogIndex, long lastLogTerm )
            {
                super( from, Type.VOTE_REQUEST );
                this.term = term;
                this.candidate = candidate;
                this.lastLogIndex = lastLogIndex;
                this.lastLogTerm = lastLogTerm;
            }

            @Override
            public long term()
            {
                return term;
            }

            @Override
            public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                Request request = (Request) o;
                return lastLogIndex == request.lastLogIndex &&
                        lastLogTerm == request.lastLogTerm &&
                        term == request.term &&
                        candidate.equals( request.candidate );
            }

            @Override
            public int hashCode()
            {
                int result = (int) term;
                result = 31 * result + candidate.hashCode();
                result = 31 * result + (int) (lastLogIndex ^ (lastLogIndex >>> 32));
                result = 31 * result + (int) (lastLogTerm ^ (lastLogTerm >>> 32));
                return result;
            }

            @Override
            public String toString()
            {
                return format( "Vote.Request from %s {term=%d, candidate=%s, lastAppended=%d, lastLogTerm=%d}",
                        from, term, candidate, lastLogIndex, lastLogTerm );
            }

            @Override
            public long lastLogTerm()
            {
                return lastLogTerm;
            }

            @Override
            public long lastLogIndex()
            {
                return lastLogIndex;
            }

            @Override
            public MemberId candidate()
            {
                return candidate;
            }
        }

        class Response extends BaseRaftMessage implements AnyVote.Response
        {
            private long term;
            private boolean voteGranted;

            public Response( MemberId from, long term, boolean voteGranted )
            {
                super( from, Type.VOTE_RESPONSE );
                this.term = term;
                this.voteGranted = voteGranted;
            }

            @Override
            public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }

                Response response = (Response) o;

                return term == response.term && voteGranted == response.voteGranted;

            }

            @Override
            public int hashCode()
            {
                int result = (int) term;
                result = 31 * result + (voteGranted ? 1 : 0);
                return result;
            }

            @Override
            public String toString()
            {
                return format( "Vote.Response from %s {term=%d, voteGranted=%s}", from, term, voteGranted );
            }

            @Override
            public long term()
            {
                return term;
            }

            @Override
            public boolean voteGranted()
            {
                return voteGranted;
            }
        }
    }

    interface PreVote
    {
        class Request extends BaseRaftMessage implements AnyVote.Request
        {
            private long term;
            private MemberId candidate;
            private long lastLogIndex;
            private long lastLogTerm;

            public Request( MemberId from, long term, MemberId candidate, long lastLogIndex, long lastLogTerm )
            {
                super( from, Type.PRE_VOTE_REQUEST );
                this.term = term;
                this.candidate = candidate;
                this.lastLogIndex = lastLogIndex;
                this.lastLogTerm = lastLogTerm;
            }

            @Override
            public long term()
            {
                return term;
            }

            @Override
            public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                Request request = (Request) o;
                return lastLogIndex == request.lastLogIndex &&
                        lastLogTerm == request.lastLogTerm &&
                        term == request.term &&
                        candidate.equals( request.candidate );
            }

            @Override
            public int hashCode()
            {
                int result = (int) term;
                result = 31 * result + candidate.hashCode();
                result = 31 * result + (int) (lastLogIndex ^ (lastLogIndex >>> 32));
                result = 31 * result + (int) (lastLogTerm ^ (lastLogTerm >>> 32));
                return result;
            }

            @Override
            public String toString()
            {
                return format( "PreVote.Request from %s {term=%d, candidate=%s, lastAppended=%d, lastLogTerm=%d}",
                        from, term, candidate, lastLogIndex, lastLogTerm );
            }

            @Override
            public long lastLogTerm()
            {
                return lastLogTerm;
            }

            @Override
            public long lastLogIndex()
            {
                return lastLogIndex;
            }

            @Override
            public MemberId candidate()
            {
                return candidate;
            }
        }

        class Response extends BaseRaftMessage implements AnyVote.Response
        {
            private long term;
            private boolean voteGranted;

            public Response( MemberId from, long term, boolean voteGranted )
            {
                super( from, Type.PRE_VOTE_RESPONSE );
                this.term = term;
                this.voteGranted = voteGranted;
            }

            @Override
            public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }

                Response response = (Response) o;

                return term == response.term && voteGranted == response.voteGranted;

            }

            @Override
            public int hashCode()
            {
                int result = (int) term;
                result = 31 * result + (voteGranted ? 1 : 0);
                return result;
            }

            @Override
            public String toString()
            {
                return format( "PreVote.Response from %s {term=%d, voteGranted=%s}", from, term, voteGranted );
            }

            @Override
            public long term()
            {
                return term;
            }

            @Override
            public boolean voteGranted()
            {
                return voteGranted;
            }
        }
    }

    interface AppendEntries
    {
        class Request extends BaseRaftMessage
        {
            private long leaderTerm;
            private long prevLogIndex;
            private long prevLogTerm;
            private RaftLogEntry[] entries;
            private long leaderCommit;

            public Request( MemberId from, long leaderTerm, long prevLogIndex, long prevLogTerm, RaftLogEntry[] entries, long leaderCommit )
            {
                super( from, Type.APPEND_ENTRIES_REQUEST );
                Objects.requireNonNull( entries );
                assert !((prevLogIndex == -1 && prevLogTerm != -1) || (prevLogTerm == -1 && prevLogIndex != -1)) :
                        format( "prevLogIndex was %d and prevLogTerm was %d", prevLogIndex, prevLogTerm );
                this.entries = entries;
                this.leaderTerm = leaderTerm;
                this.prevLogIndex = prevLogIndex;
                this.prevLogTerm = prevLogTerm;
                this.leaderCommit = leaderCommit;
            }

            public long leaderTerm()
            {
                return leaderTerm;
            }

            public long prevLogIndex()
            {
                return prevLogIndex;
            }

            public long prevLogTerm()
            {
                return prevLogTerm;
            }

            public RaftLogEntry[] entries()
            {
                return entries;
            }

            public long leaderCommit()
            {
                return leaderCommit;
            }

            @Override
            public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                Request request = (Request) o;
                return Objects.equals( leaderTerm, request.leaderTerm ) &&
                        Objects.equals( prevLogIndex, request.prevLogIndex ) &&
                        Objects.equals( prevLogTerm, request.prevLogTerm ) &&
                        Objects.equals( leaderCommit, request.leaderCommit ) &&
                        Arrays.equals( entries, request.entries );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( leaderTerm, prevLogIndex, prevLogTerm, Arrays.hashCode( entries ), leaderCommit );
            }

            @Override
            public String toString()
            {
                return format( "AppendEntries.Request from %s {leaderTerm=%d, prevLogIndex=%d, " +
                                "prevLogTerm=%d, entry=%s, leaderCommit=%d}",
                        from, leaderTerm, prevLogIndex, prevLogTerm, Arrays.toString( entries ), leaderCommit );
            }
        }

        class Response extends BaseRaftMessage
        {
            private long term;
            private boolean success;
            private long matchIndex;
            private long appendIndex;

            public Response( MemberId from, long term, boolean success, long matchIndex, long appendIndex )
            {
                super( from, Type.APPEND_ENTRIES_RESPONSE );
                this.term = term;
                this.success = success;
                this.matchIndex = matchIndex;
                this.appendIndex = appendIndex;
            }

            public long term()
            {
                return term;
            }

            public boolean success()
            {
                return success;
            }

            public long matchIndex()
            {
                return matchIndex;
            }

            public long appendIndex()
            {
                return appendIndex;
            }

            @Override
            public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                if ( !super.equals( o ) )
                {
                    return false;
                }
                Response response = (Response) o;
                return term == response.term &&
                        success == response.success &&
                        matchIndex == response.matchIndex &&
                        appendIndex == response.appendIndex;
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( super.hashCode(), term, success, matchIndex, appendIndex );
            }

            @Override
            public String toString()
            {
                return format( "AppendEntries.Response from %s {term=%d, success=%s, matchIndex=%d, appendIndex=%d}",
                        from, term, success, matchIndex, appendIndex );
            }
        }
    }

    class Heartbeat extends BaseRaftMessage
    {
        private long leaderTerm;
        private long commitIndex;
        private long commitIndexTerm;

        public Heartbeat( MemberId from, long leaderTerm, long commitIndex, long commitIndexTerm )
        {
            super( from, Type.HEARTBEAT );
            this.leaderTerm = leaderTerm;
            this.commitIndex = commitIndex;
            this.commitIndexTerm = commitIndexTerm;
        }

        public long leaderTerm()
        {
            return leaderTerm;
        }

        public long commitIndex()
        {
            return commitIndex;
        }

        public long commitIndexTerm()
        {
            return commitIndexTerm;
        }

        @Override
        public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
        {
            return handler.handle( this );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            if ( !super.equals( o ) )
            {
                return false;
            }

            Heartbeat heartbeat = (Heartbeat) o;

            return leaderTerm == heartbeat.leaderTerm &&
                   commitIndex == heartbeat.commitIndex &&
                   commitIndexTerm == heartbeat.commitIndexTerm;
        }

        @Override
        public int hashCode()
        {
            int result = super.hashCode();
            result = 31 * result + (int) (leaderTerm ^ (leaderTerm >>> 32));
            result = 31 * result + (int) (commitIndex ^ (commitIndex >>> 32));
            result = 31 * result + (int) (commitIndexTerm ^ (commitIndexTerm >>> 32));
            return result;
        }

        @Override
        public String toString()
        {
            return format( "Heartbeat from %s {leaderTerm=%d, commitIndex=%d, commitIndexTerm=%d}", from, leaderTerm,
                    commitIndex, commitIndexTerm );
        }
    }

    class HeartbeatResponse extends BaseRaftMessage
    {

        public HeartbeatResponse( MemberId from )
        {
            super( from, HEARTBEAT_RESPONSE );
        }

        @Override
        public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
        {
            return handler.handle( this );
        }

        @Override
        public String toString()
        {
            return "HeartbeatResponse{from=" + from + "}";
        }
    }

    class LogCompactionInfo extends BaseRaftMessage
    {
        private long leaderTerm;
        private long prevIndex;

        public LogCompactionInfo( MemberId from, long leaderTerm, long prevIndex )
        {
            super( from, Type.LOG_COMPACTION_INFO );
            this.leaderTerm = leaderTerm;
            this.prevIndex = prevIndex;
        }

        public long leaderTerm()
        {
            return leaderTerm;
        }

        public long prevIndex()
        {
            return prevIndex;
        }

        @Override
        public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
        {
            return handler.handle( this );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            if ( !super.equals( o ) )
            {
                return false;
            }

            LogCompactionInfo other = (LogCompactionInfo) o;

            return leaderTerm == other.leaderTerm &&
                   prevIndex == other.prevIndex;
        }

        @Override
        public int hashCode()
        {
            int result = super.hashCode();
            result = 31 * result + (int) (leaderTerm ^ (leaderTerm >>> 32));
            result = 31 * result + (int) (prevIndex ^ (prevIndex >>> 32));
            return result;
        }

        @Override
        public String toString()
        {
            return format( "Log compaction from %s {leaderTerm=%d, prevIndex=%d}", from, leaderTerm, prevIndex );
        }
    }

    interface Timeout
    {
        class Election extends BaseRaftMessage
        {
            public Election( MemberId from )
            {
                super( from, Type.ELECTION_TIMEOUT );
            }

            @Override
            public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public String toString()
            {
                return "Timeout.Election{}";
            }
        }

        class Heartbeat extends BaseRaftMessage
        {
            public Heartbeat( MemberId from )
            {
                super( from, Type.HEARTBEAT_TIMEOUT );
            }

            @Override
            public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public String toString()
            {
                return "Timeout.Heartbeat{}";
            }
        }
    }

    interface NewEntry
    {
        class Request extends BaseRaftMessage
        {
            private ReplicatedContent content;

            public Request( MemberId from, ReplicatedContent content )
            {
                super( from, Type.NEW_ENTRY_REQUEST );
                this.content = content;
            }

            @Override
            public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public String toString()
            {
                return format( "NewEntry.Request from %s {content=%s}", from, content );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }

                Request request = (Request) o;

                return !(content != null ? !content.equals( request.content ) : request.content != null);
            }

            @Override
            public int hashCode()
            {
                return content != null ? content.hashCode() : 0;
            }

            public ReplicatedContent content()
            {
                return content;
            }
        }

        class BatchRequest extends BaseRaftMessage
        {
            private List<ReplicatedContent> list;

            public BatchRequest( int batchSize )
            {
                super( null, Type.NEW_BATCH_REQUEST );
                list = new ArrayList<>( batchSize );
            }

            public void add( ReplicatedContent content )
            {
                list.add( content );
            }

            @Override
            public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
            {
                return handler.handle( this );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                if ( !super.equals( o ) )
                {
                    return false;
                }
                BatchRequest batchRequest = (BatchRequest) o;
                return Objects.equals( list, batchRequest.list );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( super.hashCode(), list );
            }

            @Override
            public String toString()
            {
                return "BatchRequest{" +
                       "list=" + list +
                       '}';
            }

            public List<ReplicatedContent> contents()
            {
                return Collections.unmodifiableList( list );
            }
        }
    }

    interface EnrichedRaftMessage<RM extends RaftMessage> extends RaftMessage
    {
        RM message();

        @Override
        default MemberId from()
        {
            return message().from();
        }

        @Override
        default Type type()
        {
            return message().type();
        }

        @Override
        default <T, E extends Exception> T dispatch( Handler<T, E> handler ) throws E
        {
            return message().dispatch( handler );
        }
    }

    interface ClusterIdAwareMessage<RM extends RaftMessage> extends EnrichedRaftMessage<RM>
    {
        ClusterId clusterId();

        static <RM extends RaftMessage> ClusterIdAwareMessage<RM> of( ClusterId clusterId, RM message )
        {
            return new ClusterIdAwareMessageImpl<>( clusterId, message );
        }
    }

    interface ReceivedInstantAwareMessage<RM extends RaftMessage> extends EnrichedRaftMessage<RM>
    {
        Instant receivedAt();

        static <RM extends RaftMessage> ReceivedInstantAwareMessage<RM> of( Instant receivedAt, RM message )
        {
            return new ReceivedInstantAwareMessageImpl<>( receivedAt, message );
        }
    }

    interface ReceivedInstantClusterIdAwareMessage<RM extends RaftMessage> extends ReceivedInstantAwareMessage<RM>, ClusterIdAwareMessage<RM>
    {
        static <RM extends RaftMessage> ReceivedInstantClusterIdAwareMessage<RM> of( Instant receivedAt, ClusterId clusterId, RM message )
        {
            return new ReceivedInstantClusterIdAwareMessageImpl<>( receivedAt, clusterId, message );
        }
    }

    class ClusterIdAwareMessageImpl<RM extends RaftMessage> implements ClusterIdAwareMessage<RM>
    {
        private final ClusterId clusterId;
        private final RM message;

        private ClusterIdAwareMessageImpl( ClusterId clusterId, RM message )
        {
            Objects.requireNonNull( message );
            this.clusterId = clusterId;
            this.message = message;
        }

        public ClusterId clusterId()
        {
            return clusterId;
        }

        @Override
        public RM message()
        {
            return message;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ClusterIdAwareMessageImpl<?> that = (ClusterIdAwareMessageImpl<?>) o;
            return Objects.equals( clusterId, that.clusterId ) && Objects.equals( message(), that.message() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( clusterId, message() );
        }

        @Override
        public String toString()
        {
            return format( "{clusterId: %s, message: %s}", clusterId, message() );
        }
    }

    class ReceivedInstantAwareMessageImpl<RM extends RaftMessage> implements ReceivedInstantAwareMessage<RM>
    {
        private final Instant receivedAt;
        private final RM message;

        private ReceivedInstantAwareMessageImpl( Instant receivedAt, RM message )
        {
            Objects.requireNonNull( message );
            this.receivedAt = receivedAt;
            this.message = message;
        }

        @Override
        public Instant receivedAt()
        {
            return receivedAt;
        }

        @Override
        public RM message()
        {
            return message;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ReceivedInstantAwareMessageImpl<?> that = (ReceivedInstantAwareMessageImpl<?>) o;
            return Objects.equals( receivedAt, that.receivedAt ) && Objects.equals( message(), that.message() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( receivedAt, message() );
        }

        @Override
        public String toString()
        {
            return format( "{receivedAt: %s, message: %s}", receivedAt, message() );
        }
    }

    class ReceivedInstantClusterIdAwareMessageImpl<RM extends RaftMessage> implements ReceivedInstantClusterIdAwareMessage<RM>
    {
        private final Instant receivedAt;
        private final ClusterId clusterId;
        private final RM message;

        private ReceivedInstantClusterIdAwareMessageImpl( Instant receivedAt, ClusterId clusterId, RM message )
        {
            Objects.requireNonNull( message );
            this.clusterId = clusterId;
            this.receivedAt = receivedAt;
            this.message = message;
        }

        @Override
        public Instant receivedAt()
        {
            return receivedAt;
        }

        @Override
        public ClusterId clusterId()
        {
            return clusterId;
        }

        @Override
        public RM message()
        {
            return message;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ReceivedInstantClusterIdAwareMessageImpl<?> that = (ReceivedInstantClusterIdAwareMessageImpl<?>) o;
            return Objects.equals( receivedAt, that.receivedAt ) && Objects.equals( clusterId, that.clusterId ) && Objects.equals( message(), that.message() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( receivedAt, clusterId, message() );
        }

        @Override
        public String toString()
        {
            return format( "{clusterId: %s, receivedAt: %s, message: %s}", clusterId, receivedAt, message() );
        }
    }

    class PruneRequest extends BaseRaftMessage
    {
        private final long pruneIndex;

        public PruneRequest( long pruneIndex )
        {
            super( null, PRUNE_REQUEST );
            this.pruneIndex = pruneIndex;
        }

        public long pruneIndex()
        {
            return pruneIndex;
        }

        @Override
        public <T,E extends Exception> T dispatch( Handler<T,E> handler ) throws E
        {
            return handler.handle( this );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            if ( !super.equals( o ) )
            {
                return false;
            }
            PruneRequest that = (PruneRequest) o;
            return pruneIndex == that.pruneIndex;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( super.hashCode(), pruneIndex );
        }
    }

    abstract class BaseRaftMessage implements RaftMessage
    {
        protected final MemberId from;
        private final Type type;

        BaseRaftMessage( MemberId from, Type type )
        {
            this.from = from;
            this.type = type;
        }

        @Override
        public MemberId from()
        {
            return from;
        }

        @Override
        public Type type()
        {
            return type;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            BaseRaftMessage that = (BaseRaftMessage) o;
            return Objects.equals( from, that.from ) && type == that.type;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( from, type );
        }
    }
}
