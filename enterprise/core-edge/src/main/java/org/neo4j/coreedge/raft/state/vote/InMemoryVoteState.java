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
package org.neo4j.coreedge.raft.state.vote;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.neo4j.coreedge.raft.state.membership.Marshal;

public class InMemoryVoteState<MEMBER> implements VoteState<MEMBER>
{
    private MEMBER votedFor;
    private long term = -1;

    public InMemoryVoteState()
    {
    }

    public InMemoryVoteState( MEMBER votedFor, long term )
    {
        this.term = term;
        this.votedFor = votedFor;
    }

    public InMemoryVoteState( InMemoryVoteState<MEMBER> inMemoryVoteState )
    {
        this.votedFor = inMemoryVoteState.votedFor;
        this.term = inMemoryVoteState.term;
    }

    @Override
    public MEMBER votedFor()
    {
        return votedFor;
    }

    @Override
    public void votedFor( MEMBER votedFor, long term )
    {
        assert ensureVoteIsUniquePerTerm( votedFor, term ) : "Votes for any instance should always be in more recent terms";

        this.votedFor = votedFor;
        this.term = term;
    }

    private boolean ensureVoteIsUniquePerTerm( MEMBER votedFor, long term )
    {
        if ( votedFor == null && this.votedFor == null )
        {
            return true;
        }
        else if ( votedFor == null )
        {
            return term > this.term;
        }
        else if ( this.votedFor == null )
        {
            return term > this.term;
        }
        else
        {
            return this.votedFor.equals( votedFor ) || term > this.term;
        }
    }

    @Override
    public long term()
    {
        return term;
    }

    public static class InMemoryVoteStateMarshal<CoreMember>
            implements Marshal<InMemoryVoteState<CoreMember>>

    {
        public static final int NUMBER_OF_BYTES_PER_VOTE = 100_000; // 100kB URI max
        private final Marshal<CoreMember> marshal;

        public InMemoryVoteStateMarshal( Marshal<CoreMember> marshal )
        {
            this.marshal = marshal;
        }

        @Override
        public void marshal( InMemoryVoteState<CoreMember> state, ByteBuffer buffer )
        {
            buffer.putLong( state.term );
            marshal.marshal( state.votedFor(), buffer );
        }

        @Override
        public InMemoryVoteState<CoreMember> unmarshal( ByteBuffer source )
        {
            try
            {
                final long term = source.getLong();
                final CoreMember member = marshal.unmarshal( source );

                if ( member == null )
                {
                    return null;
                }

                return new InMemoryVoteState<>( member, term );
            }
            catch ( BufferUnderflowException ex )
            {
                return null;
            }
        }
    }
}
