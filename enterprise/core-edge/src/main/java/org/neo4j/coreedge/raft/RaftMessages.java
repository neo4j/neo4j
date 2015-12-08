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
package org.neo4j.coreedge.raft;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;

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

        // Timeouts
        ELECTION_TIMEOUT,
        HEARTBEAT_TIMEOUT,

        // TODO: Refactor, these are client-facing messages / api. Perhaps not public and instantiated through an api
        // TODO: method instead?
        NEW_ENTRY_REQUEST,

        NEW_MEMBERSHIP_TARGET,
    }

    interface Message<MEMBER> extends Serializable
    {
        MEMBER from();

        Type type();
    }

    class Directed<MEMBER>
    {
        MEMBER to;
        Message<MEMBER> message;

        public Directed( MEMBER to, Message<MEMBER> message )
        {
            this.to = to;
            this.message = message;
        }

        public MEMBER to()
        {
            return to;
        }

        public Message<MEMBER> message()
        {
            return message;
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

            public Request( MEMBER from, long term, MEMBER candidate, long lastLogIndex, long lastLogTerm )
            {
                super( from, Type.VOTE_REQUEST );
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
                return format( "Vote.Request{term=%d, candidate=%s, lastAppended=%d, lastLogTerm=%d}",
                        term, candidate, lastLogIndex, lastLogTerm );
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

            public Response( MEMBER from, long term, boolean voteGranted )
            {
                super( from, Type.VOTE_RESPONSE );
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
                return format( "Vote.Response{from=%s, term=%d, voteGranted=%s}", from(), term, voteGranted );
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
                            RaftLogEntry[] entries, long leaderCommit )
            {
                super( from, Type.APPEND_ENTRIES_REQUEST );
                Objects.requireNonNull( entries );
                assert !((prevLogIndex == -1 && prevLogTerm != -1) || (prevLogTerm == -1 && prevLogIndex != -1));
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
                        super.from(), leaderTerm, prevLogIndex, prevLogTerm, Arrays.toString( entries ), leaderCommit );
            }
        }

        class Response<MEMBER> extends BaseMessage<MEMBER>
        {
            private long term;
            private boolean success;
            private long matchIndex;
            private long appendIndex;

            public Response( MEMBER from, long term, boolean success, long matchIndex, long appendIndex )
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
            public boolean equals( Object o )
            {
                if ( this == o )
                { return true; }
                if ( o == null || getClass() != o.getClass() )
                { return false; }
                if ( !super.equals( o ) )
                { return false; }
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
                return String.format( "Response{term=%d, success=%s, matchIndex=%d, appendIndex=%d}",
                        term, success, matchIndex, appendIndex );
            }
        }
    }

    class Heartbeat<MEMBER> extends BaseMessage<MEMBER>
    {
        private final long leaderTerm;
        private final long commitIndex;
        private final long commitIndexTerm;

        public Heartbeat( MEMBER from, long leaderTerm, long commitIndex, long commitIndexTerm )
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

            if ( leaderTerm != heartbeat.leaderTerm )
            {
                return false;
            }
            if ( commitIndex != heartbeat.commitIndex )
            {
                return false;
            }
            return commitIndexTerm == heartbeat.commitIndexTerm;

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
            return format( "Heartbeat{leaderTerm=%d, commitIndex=%d, commitIndexTerm=%d}", leaderTerm,
                    commitIndex, commitIndexTerm );
        }
    }

    interface Timeout
    {
        class Election<MEMBER> extends BaseMessage<MEMBER>
        {
            public Election( MEMBER from )
            {
                super( from, Type.ELECTION_TIMEOUT );
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
                super( from, Type.HEARTBEAT_TIMEOUT );
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

            public Request( MEMBER from, ReplicatedContent content )
            {
                super( from, Type.NEW_ENTRY_REQUEST );
                this.content = content;
            }

            @Override
            public String toString()
            {
                return format( "NewEntry.Request{content=%s}", content );
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
    }

    abstract class BaseMessage<MEMBER> implements Message<MEMBER>
    {
        private MEMBER from;
        private Type type;

        public BaseMessage( MEMBER from, Type type )
        {
            this.from = from;
            this.type = type;
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
                    Objects.equals( type, that.type );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( from, type );
        }
    }
}
