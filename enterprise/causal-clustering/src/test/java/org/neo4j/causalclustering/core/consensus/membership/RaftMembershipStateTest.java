/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.membership;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.Set;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class RaftMembershipStateTest
{
    private RaftMembershipState state = new RaftMembershipState();

    private Set<MemberId> membersA = asSet( member( 0 ), member( 1 ), member( 2 ) );
    private Set<MemberId> membersB = asSet( member( 0 ), member( 1 ), member( 2 ), member( 3 ) );

    @Test
    public void shouldHaveCorrectInitialState()
    {
        assertThat( state.getLatest(), hasSize( 0 ) );
        assertFalse( state.uncommittedMemberChangeInLog() );
    }

    @Test
    public void shouldUpdateLatestOnAppend()
    {
        // when
        state.append( 0, membersA );

        // then
        assertEquals( state.getLatest(), membersA );

        // when
        state.append( 1, membersB );

        // then
        assertEquals( state.getLatest(), membersB );
        assertEquals( 1, state.getOrdinal() );
    }

    @Test
    public void shouldKeepLatestOnCommit()
    {
        // given
        state.append( 0, membersA );
        state.append( 1, membersB );

        // when
        state.commit( 0 );

        // then
        assertEquals( state.getLatest(), membersB );
        assertTrue( state.uncommittedMemberChangeInLog() );
        assertEquals( 1, state.getOrdinal() );
    }

    @Test
    public void shouldLowerUncommittedFlagOnCommit()
    {
        // given
        state.append( 0, membersA );
        assertTrue( state.uncommittedMemberChangeInLog() );

        // when
        state.commit( 0 );

        // then
        assertFalse( state.uncommittedMemberChangeInLog() );
    }

    @Test
    public void shouldRevertToCommittedStateOnTruncation()
    {
        // given
        state.append( 0, membersA );
        state.commit( 0 );
        state.append( 1, membersB );
        assertEquals( state.getLatest(), membersB );

        // when
        state.truncate( 1 );

        // then
        assertEquals( state.getLatest(), membersA );
        assertEquals( 3, state.getOrdinal() );
    }

    @Test
    public void shouldNotTruncateEarlierThanIndicated()
    {
        // given
        state.append( 0, membersA );
        state.append( 1, membersB );
        assertEquals( state.getLatest(), membersB );

        // when
        state.truncate( 2 );

        // then
        assertEquals( state.getLatest(), membersB );
        assertEquals( 1, state.getOrdinal() );
    }

    @Test
    public void shouldMarshalCorrectly() throws Exception
    {
        // given
        RaftMembershipState.Marshal marshal = new RaftMembershipState.Marshal();
        state = new RaftMembershipState( 5, new MembershipEntry( 7, membersA ), new MembershipEntry( 8, membersB ) );

        // when
        ByteBuf buffer = Unpooled.buffer( 1_000 );
        marshal.marshal( state, new NetworkFlushableChannelNetty4( buffer ) );
        final RaftMembershipState recovered = marshal.unmarshal( new NetworkReadableClosableChannelNetty4( buffer ) );

        // then
        assertEquals( state, recovered );
    }

    @Test
    public void shouldRefuseToAppendToTheSameIndexTwice()
    {
        // given
        state.append( 0, membersA );
        state.append( 1, membersB );

        // when
        boolean reAppendA = state.append( 0, membersA );
        boolean reAppendB = state.append( 1, membersB );

        // then
        assertFalse( reAppendA );
        assertFalse( reAppendB );
        assertEquals( membersA, state.committed().members() );
        assertEquals( membersB, state.getLatest() );
    }
}
