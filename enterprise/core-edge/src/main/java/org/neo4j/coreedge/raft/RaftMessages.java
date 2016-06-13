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
package org.neo4j.coreedge.raft;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.neo4j.coreedge.network.Message;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.kernel.impl.store.StoreId;

import static java.lang.String.format;

public interface RaftMessages
{
    enum Type
    {
        VOTE_REQUEST,
        VOTE_RESPONSE,

        APPEND_ENTRIES_REQUEST,
        APPEND_ENTRIES_RESPONSE,

        HEARTBEAT,
        LOG_COMPACTION_INFO,

        // Timeouts
        ELECTION_TIMEOUT,
        HEARTBEAT_TIMEOUT,

        // TODO: Refactor, these are client-facing messages / api. Perhaps not public and instantiated through an api
        // TODO: method instead?
        NEW_ENTRY_REQUEST,
        NEW_BATCH_REQUEST,
        NEW_MEMBERSHIP_TARGET,
    }

    interface RaftMessage<MEMBER> extends Message
    {
        MEMBER from();
        Type type();
        StoreId storeId();
    }

    class Directed<MEMBER>
    {
        MEMBER to;
        RaftMessage<MEMBER> message;

        public Directed( MEMBER to, RaftMessage<MEMBER> message )
        {
            this.to = to;
            this.message = message;
        }

        public MEMBER to()
        {
            return to;
        }

        public RaftMessage<MEMBER> message()
        {
            return message;
        }

        @Override
        public String toString()
        {
            return format( "Directed{to=%s, message=%s}", to, message );
        }
    }

    interface Vote
    {
        class Request<MEMBER> extends BaseMessage<MEMBER>
        {
            private long term;
            private MEMBER candidate;
            private long lastLogIndex;
            private long lastLogTerm;

            public Request( MEMBER from, long term, MEMBER candidate, long lastLogIndex, long lastLogTerm,
                    StoreId storeId)
            {
                super( from, Type.VOTE_REQUEST, storeId );
                this.term = term;
                this.candidate = candidate;
                this.lastLogIndex = lastLogIndex;
                this.lastLogTerm = lastLogTerm;
            }

            public long term()
            {
                return term;
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

            public long lastLogTerm()
            {
                return lastLogTerm;
            }

            public long lastLogIndex()
            {
                return lastLogIndex;
            }

            public MEMBER candidate()
            {
                return candidate;
            }
        }

        class Response<MEMBER> extends BaseMessage<MEMBER>
        {
            private long term;
            private boolean voteGranted;

            public Response( MEMBER from, long term, boolean voteGranted, StoreId storeId )
            {
                super( from, Type.VOTE_RESPONSE, storeId );
                this.term = term;
                this.voteGranted = voteGranted;
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

            public long term()
            {
                return term;
            }

            public boolean voteGranted()
            {
                return voteGranted;
            }
        }
    }

    interface AppendEntries
    {
        class Request<MEMBER> extends BaseMessage<MEMBER>
        {
            private long leaderTerm;
            private long prevLogIndex;
            private long prevLogTerm;
            private RaftLogEntry[] entries;
            private long leaderCommit;

            public Request( MEMBER from, long leaderTerm, long prevLogIndex, long prevLogTerm,
                            RaftLogEntry[] entries, long leaderCommit, StoreId storeId )
            {
                super( from, Type.APPEND_ENTRIES_REQUEST, storeId );
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
                Request<?> request = (Request<?>) o;
                return Objects.equals( leaderTerm, request.leaderTerm ) &&
                        Objects.equals( prevLogIndex, request.prevLogIndex ) &&
                        Objects.equals( prevLogTerm, request.prevLogTerm ) &&
                        Objects.equals( leaderCommit, request.leaderCommit ) &&
                        Arrays.equals( entries, request.entries );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( leaderTerm, prevLogIndex, prevLogTerm, entries, leaderCommit );
            }

            @Override
            public String toString()
            {
                return format( "AppendEntries.Request from %s {leaderTerm=%d, prevLogIndex=%d, " +
                                "prevLogTerm=%d, entry=%s, leaderCommit=%d}",
                        from, leaderTerm, prevLogIndex, prevLogTerm, Arrays.toString( entries ), leaderCommit );
            }
        }

        class Response<MEMBER> extends BaseMessage<MEMBER>
        {
            private long term;
            private boolean success;
            private long matchIndex;
            private long appendIndex;

            public Response( MEMBER from, long term, boolean success, long matchIndex, long appendIndex,
                             StoreId storeId )
            {
                super( from, Type.APPEND_ENTRIES_RESPONSE, storeId );
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
                Response<?> response = (Response<?>) o;
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
                return format( "AppendEntries.Response from %s {term=%d, storeId=%s, success=%s, matchIndex=%d, appendIndex=%d}",
                        from, term, storeId(), success, matchIndex, appendIndex );
            }
        }
    }

    class Heartbeat<MEMBER> extends BaseMessage<MEMBER>
    {
        private long leaderTerm;
        private long commitIndex;
        private long commitIndexTerm;

        public Heartbeat( MEMBER from, long leaderTerm, long commitIndex, long commitIndexTerm, StoreId storeId )
        {
            super( from, Type.HEARTBEAT, storeId );
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

            Heartbeat<?> heartbeat = (Heartbeat<?>) o;

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

    class LogCompactionInfo<MEMBER> extends BaseMessage<MEMBER>
    {
        private long leaderTerm;
        private long prevIndex;

        public LogCompactionInfo( MEMBER from, long leaderTerm, long prevIndex, StoreId storeId )
        {
            super( from, Type.LOG_COMPACTION_INFO, storeId );
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

            LogCompactionInfo<?> other = (LogCompactionInfo<?>) o;

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
        class Election<MEMBER> extends BaseMessage<MEMBER>
        {
            public Election( MEMBER from, StoreId storeId )
            {
                super( from, Type.ELECTION_TIMEOUT, storeId );
            }

            @Override
            public String toString()
            {
                return "Timeout.Election{}";
            }
        }

        class Heartbeat<MEMBER> extends BaseMessage<MEMBER>
        {
            public Heartbeat( MEMBER from )
            {
                super( from, Type.HEARTBEAT_TIMEOUT, null );
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
        class Request<MEMBER> extends BaseMessage<MEMBER>
        {
            private ReplicatedContent content;

            public Request( MEMBER from, ReplicatedContent content, StoreId storeId )
            {
                super( from, Type.NEW_ENTRY_REQUEST, storeId );
                this.content = content;
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

        class Batch<MEMBER> extends BaseMessage<MEMBER>
        {
            private List<ReplicatedContent> list;

            public Batch( int batchSize, StoreId storeId )
            {
                super( null, Type.NEW_BATCH_REQUEST, storeId );
                list = new ArrayList<>( batchSize );
            }

            public void add( ReplicatedContent content )
            {
                list.add( content );
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                { return true; }
                if ( o == null || getClass() != o.getClass() )
                { return false; }
                if ( !super.equals( o ) )
                { return false; }
                Batch<?> batch = (Batch<?>) o;
                return Objects.equals( list, batch.list );
            }

            @Override
            public int hashCode()
            {
                return Objects.hash( super.hashCode(), list );
            }

            @Override
            public String toString()
            {
                return "Batch{" +
                       "list=" + list +
                       '}';
            }

            public List<ReplicatedContent> contents()
            {
                return Collections.unmodifiableList( list );
            }
        }
    }

    abstract class BaseMessage<MEMBER> implements RaftMessage<MEMBER>
    {
        protected MEMBER from;
        private Type type;
        private StoreId storeId;

        public BaseMessage( MEMBER from, Type type, StoreId storeId )
        {
            this.from = from;
            this.type = type;
            this.storeId = storeId;
        }

        @Override
        public MEMBER from()
        {
            return from;
        }

        @Override
        public Type type()
        {
            return type;
        }

        @Override
        public StoreId storeId()
        {
            return storeId;
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
            BaseMessage<?> that = (BaseMessage<?>) o;
            return Objects.equals( from, that.from ) &&
                    type == that.type &&
                    ((storeId == that.storeId) || (storeId != null && storeId.theRealEquals( that.storeId )));
        }

        @Override
        public int hashCode()
        {
            int result = storeId == null ? 0 : storeId.theRealHashCode();
            return 31 * result + Objects.hash( from, type  );
        }
    }
}
