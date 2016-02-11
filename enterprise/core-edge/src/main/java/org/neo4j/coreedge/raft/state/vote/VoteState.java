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

import java.io.IOException;

import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.coreedge.raft.state.StateMarshal;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class VoteState<MEMBER>
{
    private MEMBER votedFor;
    private long term = -1;

    public VoteState()
    {
    }

    public VoteState( MEMBER votedFor, long term )
    {
        this.term = term;
        this.votedFor = votedFor;
    }

    public MEMBER votedFor()
    {
        return votedFor;
    }

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
            return true;
        }
        else
        {
            return this.votedFor.equals( votedFor ) || term > this.term;
        }
    }

    public long term()
    {
        return term;
    }

    public static class Marshal<CoreMember> implements StateMarshal<VoteState<CoreMember>>
    {
        private final ChannelMarshal<CoreMember> memberMarshal;

        public Marshal( ChannelMarshal<CoreMember> memberMarshal )
        {
            this.memberMarshal = memberMarshal;
        }

        @Override
        public void marshal( VoteState<CoreMember> state, WritableChannel channel ) throws IOException
        {
            channel.putLong( state.term );
            memberMarshal.marshal( state.votedFor(), channel );
        }

        @Override
        public VoteState<CoreMember> unmarshal( ReadableChannel source ) throws IOException
        {
            try
            {
                final long term = source.getLong();
                final CoreMember member = memberMarshal.unmarshal( source );

                if ( member == null )
                {
                    return null;
                }

                return new VoteState<>( member, term );
            }
            catch ( ReadPastEndException notEnoughBytes )
            {
                return null;
            }
        }

        @Override
        public VoteState<CoreMember> startState()
        {
            return new VoteState<>();
        }

        @Override
        public long ordinal( VoteState<CoreMember> state )
        {
            return state.term();
        }
    }
}
