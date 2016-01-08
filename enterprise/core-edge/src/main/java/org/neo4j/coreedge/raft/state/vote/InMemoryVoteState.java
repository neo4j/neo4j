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

import java.nio.ByteBuffer;

import org.neo4j.coreedge.raft.state.membership.Marshal;

public class InMemoryVoteState<MEMBER> implements VoteState<MEMBER>
{
    MEMBER votedFor;

    @Override
    public MEMBER votedFor()
    {
        return votedFor;
    }

    @Override
    public void votedFor( MEMBER votedFor )
    {
        this.votedFor = votedFor;
    }

    public static class InMemoryVoteStateStateMarshal<CoreMember>
            implements Marshal<InMemoryVoteState<CoreMember>>

    {
        public static final int NUMBER_OF_VOTES_PER_WRITE = 9999;
        private final Marshal<CoreMember> marshal;

        public InMemoryVoteStateStateMarshal( Marshal<CoreMember> marshal )
        {

            this.marshal = marshal;
        }

        @Override
        public void marshal( InMemoryVoteState<CoreMember> state, ByteBuffer buffer )
        {
            marshal.marshal( state.votedFor(), buffer );
        }

        @Override
        public InMemoryVoteState<CoreMember> unmarshal( ByteBuffer source )
        {
            final InMemoryVoteState<CoreMember> state = new InMemoryVoteState<>();
            state.votedFor( marshal.unmarshal( source ) );
            return state;
        }

    }
}
