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
package org.neo4j.coreedge.raft.state.membership;

import org.junit.Test;

import org.neo4j.coreedge.raft.membership.RaftTestGroup;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.RaftTestMarshal;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;

import static org.junit.Assert.assertEquals;

public class RaftMembershipStateTest
{
    @Test
    public void shouldSerialiseAndDeserialiseEmptyStateCorrectly() throws Exception
    {
        // given
        RaftMembershipState<CoreMember> state = new RaftMembershipState<>();
        RaftMembershipState.Marshal<CoreMember> marshal = new RaftMembershipState.Marshal<>( new CoreMember.CoreMemberMarshal() );

        // when
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        marshal.marshal( state, channel );
        final RaftMembershipState recovered = marshal.unmarshal( channel );

        // then
        assertEquals( state.votingMembers(), recovered.votingMembers() );
    }

    @Test
    public void shouldSerialiseAndDeserialiseNonEmptyStateCorrectly() throws Exception
    {
        // given
        RaftMembershipState<RaftTestMember> state = new RaftMembershipState<>();
        RaftMembershipState.Marshal<RaftTestMember> serializer = new RaftMembershipState.Marshal<>( new RaftTestMarshal() );

        RaftTestGroup coreMembers = new RaftTestGroup( 1, 2, 3 ,4 );

        state.setVotingMembers( coreMembers.getMembers() );

        // when
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        serializer.marshal( state, channel );
        final RaftMembershipState recovered = serializer.unmarshal( channel );

        // then
        assertEquals( state.votingMembers(), recovered.votingMembers() );
    }
}
