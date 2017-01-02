/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.time.Clock;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

/**
 * This class carries the core raft membership change state machine, which defines the
 * legal state transitions when adding or removing members from the raft voting group.
 *
 * The state machine has these 4 states:
 *
 * <pre>
 *   INACTIVE                    completely inactive, not leader
 *
 *   IDLE,                       leader, but idle, no work to do
 *   CATCHUP IN PROGRESS,        member catching up
 *   CONSENSUS IN PROGRESS       caught up member being added to voting group
 * </pre>
 *
 * The normal progression when adding a member is:
 * <pre>
 *   IDLE->CATCHUP->CONSENSUS->IDLE
 * </pre>
 *
 * the normal progression when removing a member is:
 * <pre>
 *   IDLE->CONSENSUS->IDLE
 * </pre>
 *
 * Only a single member change is handled at a time.
 */
class RaftMembershipChanger
{
    private final Log log;
    public RaftMembershipStateMachineEventHandler state = new Inactive();

    private final ReadableRaftLog raftLog;
    private final Clock clock;
    private final long electionTimeout;

    private final RaftMembershipManager membershipManager;
    private long catchupTimeout;

    private MemberId catchingUpMember;

    RaftMembershipChanger( ReadableRaftLog raftLog, Clock clock, long electionTimeout,
            LogProvider logProvider, long catchupTimeout, RaftMembershipManager membershipManager )
    {
        this.raftLog = raftLog;
        this.clock = clock;
        this.electionTimeout = electionTimeout;
        this.catchupTimeout = catchupTimeout;
        this.membershipManager = membershipManager;
        this.log = logProvider.getLog( getClass() );
    }

    private synchronized void handleState( RaftMembershipStateMachineEventHandler newState )
    {
        RaftMembershipStateMachineEventHandler oldState = state;
        this.state = newState;

        if ( oldState != newState )
        {
            oldState.onExit();
            newState.onEntry();

            log.info( newState.toString() );
            membershipManager.stateChanged();
        }
    }

    void onRole( Role role )
    {
        handleState( state.onRole( role ) );
    }

    void onRaftGroupCommitted()
    {
        handleState( state.onRaftGroupCommitted() );
    }

    void onFollowerStateChange( FollowerStates<MemberId> followerStates )
    {
        handleState( state.onFollowerStateChange( followerStates ) );
    }

    void onMissingMember( MemberId member )
    {
        handleState( state.onMissingMember( member ) );
    }

    void onSuperfluousMember( MemberId member )
    {
        handleState( state.onSuperfluousMember( member ) );
    }

    void onTargetChanged( Set<MemberId> targetMembers )
    {
        handleState( state.onTargetChanged( targetMembers ) );
    }

    private class Inactive extends RaftMembershipStateMachineEventHandler.Adapter
    {
        @Override
        public RaftMembershipStateMachineEventHandler onRole( Role role )
        {
            if ( role == Role.LEADER )
            {
                if ( membershipManager.uncommittedMemberChangeInLog() )
                {
                    return new ConsensusInProgress();
                }
                else
                {
                    return new Idle();
                }
            }
            return this;
        }

        @Override
        public String toString()
        {
            return "Inactive{}";
        }
    }

    abstract class ActiveBaseState extends RaftMembershipStateMachineEventHandler.Adapter
    {
        @Override
        public RaftMembershipStateMachineEventHandler onRole( Role role )
        {
            if ( role != Role.LEADER )
            {
                return new Inactive();
            }
            else
            {
                return this;
            }
        }
    }

    private class Idle extends ActiveBaseState
    {
        @Override
        public RaftMembershipStateMachineEventHandler onMissingMember( MemberId member )
        {
            return new CatchingUp( member );
        }

        @Override
        public RaftMembershipStateMachineEventHandler onSuperfluousMember( MemberId member )
        {
            Set<MemberId> updatedVotingMembers = new HashSet<>( membershipManager.votingMembers() );
            updatedVotingMembers.remove( member );
            membershipManager.doConsensus( updatedVotingMembers );

            return new ConsensusInProgress();
        }

        @Override
        public String toString()
        {
            return "Idle{}";
        }
    }

    private class CatchingUp extends ActiveBaseState
    {
        private final CatchupGoalTracker catchupGoalTracker;
        boolean movingToConsensus;

        CatchingUp( MemberId member )
        {
            this.catchupGoalTracker = new CatchupGoalTracker( raftLog, clock, electionTimeout, catchupTimeout );
            catchingUpMember = member;
        }

        @Override
        public void onEntry()
        {
            membershipManager.addAdditionalReplicationMember( catchingUpMember );
            log.info( "Adding replication member: " + catchingUpMember );
        }

        @Override
        public void onExit()
        {
            if ( !movingToConsensus )
            {
                membershipManager.removeAdditionalReplicationMember( catchingUpMember );
                log.info( "Removing replication member: " + catchingUpMember );
            }
        }

        @Override
        public RaftMembershipStateMachineEventHandler onRole( Role role )
        {
            if ( role != Role.LEADER )
            {
                return new Inactive();
            }
            else
            {
                return this;
            }
        }

        @Override
        public RaftMembershipStateMachineEventHandler onFollowerStateChange( FollowerStates<MemberId> followerStates )
        {
            catchupGoalTracker.updateProgress( followerStates.get( catchingUpMember ) );

            if ( catchupGoalTracker.isFinished() )
            {
                if ( catchupGoalTracker.isGoalAchieved() )
                {
                    Set<MemberId> updatedVotingMembers = new HashSet<>( membershipManager.votingMembers() );
                    updatedVotingMembers.add( catchingUpMember );
                    membershipManager.doConsensus( updatedVotingMembers );

                    movingToConsensus = true;
                    return new ConsensusInProgress();
                }
                else
                {
                    return new Idle();
                }
            }
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler onTargetChanged( Set targetMembers )
        {
            if ( !targetMembers.contains( catchingUpMember ) )
            {
                return new Idle();
            }
            else
            {
                return this;
            }
        }

        @Override
        public String toString()
        {
            return format( "CatchingUp{catchupGoalTracker=%s, catchingUpMember=%s}", catchupGoalTracker,
                    catchingUpMember );
        }
    }

    private class ConsensusInProgress extends ActiveBaseState
    {
        @Override
        public RaftMembershipStateMachineEventHandler onRaftGroupCommitted()
        {
            return new Idle();
        }

        @Override
        public void onEntry()
        {
        }

        @Override
        public void onExit()
        {
            membershipManager.removeAdditionalReplicationMember( catchingUpMember );
            log.info( "Removing replication member: " + catchingUpMember );
        }

        @Override
        public String toString()
        {
            return "ConsensusInProgress{}";
        }
    }
}
