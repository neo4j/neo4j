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
package org.neo4j.causalclustering.core.consensus.state;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import org.neo4j.causalclustering.core.consensus.outcome.AppendLogEntry;
import org.neo4j.causalclustering.core.consensus.outcome.RaftLogCommand;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.outcome.TruncateLogCommand;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import org.neo4j.causalclustering.core.consensus.term.TermState;
import org.neo4j.causalclustering.core.consensus.vote.VoteState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static org.neo4j.causalclustering.core.consensus.roles.Role.CANDIDATE;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

public class RaftStateTest
{

    @Test
    public void shouldUpdateCacheState() throws Exception
    {
        //Test that updates applied to the raft state will be reflected in the entry cache.

        //given
        InFlightCache cache = new ConsecutiveInFlightCache();
        RaftState raftState = new RaftState( member( 0 ),
                new InMemoryStateStorage<>( new TermState() ), new FakeMembership(), new InMemoryRaftLog(),
                new InMemoryStateStorage<>( new VoteState() ), cache, NullLogProvider.getInstance(), false, false );

        List<RaftLogCommand> logCommands = new LinkedList<RaftLogCommand>()
        {{
            add( new AppendLogEntry( 1, new RaftLogEntry( 0L, valueOf( 0 ) ) ) );
            add( new AppendLogEntry( 2, new RaftLogEntry( 0L, valueOf( 1 ) ) ) );
            add( new AppendLogEntry( 3, new RaftLogEntry( 0L, valueOf( 2 ) ) ) );
            add( new AppendLogEntry( 4, new RaftLogEntry( 0L, valueOf( 4 ) ) ) );
            add( new TruncateLogCommand( 3 ) );
            add( new AppendLogEntry( 3, new RaftLogEntry( 0L, valueOf( 5 ) ) ) );
        }};

        Outcome raftTestMemberOutcome =
                new Outcome( CANDIDATE, 0, null, -1, null, emptySet(), emptySet(), -1, initialFollowerStates(), true,
                        logCommands, emptyOutgoingMessages(), emptySet(), -1, emptySet(), false );

        //when
        raftState.update(raftTestMemberOutcome);

        //then
        assertNotNull( cache.get( 1L ) );
        assertNotNull( cache.get( 2L ) );
        assertNotNull( cache.get( 3L ) );
        assertEquals( valueOf( 5 ), cache.get( 3L ).content() );
        assertNull( cache.get( 4L ) );
    }

    @Test
    public void shouldRemoveFollowerStateAfterBecomingLeader() throws Exception
    {
        // given
        RaftState raftState = new RaftState( member( 0 ),
                new InMemoryStateStorage<>( new TermState() ),
                new FakeMembership(), new InMemoryRaftLog(),
                new InMemoryStateStorage<>( new VoteState( ) ),
                new ConsecutiveInFlightCache(), NullLogProvider.getInstance(),
                false, false );

        raftState.update( new Outcome( CANDIDATE, 1, null, -1, null, emptySet(), emptySet(), -1, initialFollowerStates(), true, emptyLogCommands(),
                emptyOutgoingMessages(), emptySet(), -1, emptySet(), false ) );

        // when
        raftState.update( new Outcome( CANDIDATE, 1, null, -1, null, emptySet(), emptySet(), -1, new FollowerStates<>(), true, emptyLogCommands(),
                emptyOutgoingMessages(), emptySet(), -1, emptySet(), false ) );

        // then
        assertEquals( 0, raftState.followerStates().size() );
    }

    private Collection<RaftMessages.Directed> emptyOutgoingMessages()
    {
        return new ArrayList<>();
    }

    private FollowerStates<MemberId> initialFollowerStates()
    {
        return new FollowerStates<>( new FollowerStates<>(), member( 1 ), new FollowerState() );
    }

    private Collection<RaftLogCommand> emptyLogCommands()
    {
        return Collections.emptyList();
    }

    private class FakeMembership implements RaftMembership
    {
        @Override
        public Set<MemberId> votingMembers()
        {
            return emptySet();
        }

        @Override
        public Set<MemberId> replicationMembers()
        {
            return emptySet();
        }

        @Override
        public void registerListener( Listener listener )
        {
            throw new UnsupportedOperationException();
        }
    }
}
