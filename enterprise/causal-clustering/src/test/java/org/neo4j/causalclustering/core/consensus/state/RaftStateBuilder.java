/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.state;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import org.neo4j.causalclustering.core.consensus.outcome.RaftLogCommand;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import org.neo4j.causalclustering.core.consensus.term.TermState;
import org.neo4j.causalclustering.core.consensus.vote.VoteState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.emptySet;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class RaftStateBuilder
{
    public static RaftStateBuilder raftState()
    {
        return new RaftStateBuilder();
    }

    public MemberId myself;
    private Set<MemberId> votingMembers = emptySet();
    private Set<MemberId> replicationMembers = emptySet();
    public long term;
    public MemberId leader;
    public long leaderCommit = -1;
    private MemberId votedFor;
    private RaftLog entryLog = new InMemoryRaftLog();
    private boolean supportPreVoting;
    private Set<MemberId> votesForMe = emptySet();
    private Set<MemberId> preVotesForMe = emptySet();
    private long lastLogIndexBeforeWeBecameLeader = -1;
    public long commitIndex = -1;
    private FollowerStates<MemberId> followerStates = new FollowerStates<>();
    private boolean isPreElection;
    private boolean refusesToBeLeader;

    public RaftStateBuilder myself( MemberId myself )
    {
        this.myself = myself;
        return this;
    }

    public RaftStateBuilder votingMembers( Set<MemberId> currentMembers )
    {
        this.votingMembers = currentMembers;
        return this;
    }

    private RaftStateBuilder replicationMembers( Set<MemberId> replicationMembers )
    {
        this.replicationMembers = replicationMembers;
        return this;
    }

    public RaftStateBuilder term( long term )
    {
        this.term = term;
        return this;
    }

    public RaftStateBuilder leader( MemberId leader )
    {
        this.leader = leader;
        return this;
    }

    public RaftStateBuilder leaderCommit( long leaderCommit )
    {
        this.leaderCommit = leaderCommit;
        return this;
    }

    public RaftStateBuilder votedFor( MemberId votedFor )
    {
        this.votedFor = votedFor;
        return this;
    }

    public RaftStateBuilder entryLog( RaftLog entryLog )
    {
        this.entryLog = entryLog;
        return this;
    }

    public RaftStateBuilder votesForMe( Set<MemberId> votesForMe )
    {
        this.votesForMe = votesForMe;
        return this;
    }

    public RaftStateBuilder supportsPreVoting( boolean supportPreVoting )
    {
        this.supportPreVoting = supportPreVoting;
        return this;
    }

    public RaftStateBuilder lastLogIndexBeforeWeBecameLeader( long lastLogIndexBeforeWeBecameLeader )
    {
        this.lastLogIndexBeforeWeBecameLeader = lastLogIndexBeforeWeBecameLeader;
        return this;
    }

    public RaftStateBuilder commitIndex( long commitIndex )
    {
        this.commitIndex = commitIndex;
        return this;
    }

    public RaftStateBuilder setPreElection( boolean isPreElection )
    {
        this.isPreElection = isPreElection;
        return this;
    }

    public RaftStateBuilder setRefusesToBeLeader( boolean refusesToBeLeader )
    {
        this.refusesToBeLeader = refusesToBeLeader;
        return this;
    }

    public RaftState build() throws IOException
    {
        StateStorage<TermState> termStore = new InMemoryStateStorage<>( new TermState() );
        StateStorage<VoteState> voteStore = new InMemoryStateStorage<>( new VoteState( ) );
        StubMembership membership = new StubMembership( votingMembers, replicationMembers );

        RaftState state = new RaftState( myself, termStore, membership, entryLog,
                voteStore, new ConsecutiveInFlightCache(), NullLogProvider.getInstance(), supportPreVoting, refusesToBeLeader );

        Collection<RaftMessages.Directed> noMessages = Collections.emptyList();
        List<RaftLogCommand> noLogCommands = Collections.emptyList();

        state.update( new Outcome( null, term, leader, leaderCommit, votedFor, votesForMe, preVotesForMe,
                lastLogIndexBeforeWeBecameLeader, followerStates, false, noLogCommands,
                noMessages, emptySet(), commitIndex, emptySet(), isPreElection ) );

        return state;
    }

    public RaftStateBuilder votingMembers( MemberId... members )
    {
        return votingMembers( asSet( members ) );
    }

    public RaftStateBuilder replicationMembers( MemberId... members )
    {
        return replicationMembers( asSet( members ) );
    }

    public RaftStateBuilder messagesSentToFollower( MemberId member, long nextIndex )
    {
        return this;
    }

    private static class StubMembership implements RaftMembership
    {
        private Set<MemberId> votingMembers;
        private final Set<MemberId> replicationMembers;

        private StubMembership( Set<MemberId> votingMembers, Set<MemberId> replicationMembers )
        {
            this.votingMembers = votingMembers;
            this.replicationMembers = replicationMembers;
        }

        @Override
        public Set<MemberId> votingMembers()
        {
            return votingMembers;
        }

        @Override
        public Set<MemberId> replicationMembers()
        {
            return replicationMembers;
        }

        @Override
        public void registerListener( Listener listener )
        {
            throw new UnsupportedOperationException();
        }
    }
}
