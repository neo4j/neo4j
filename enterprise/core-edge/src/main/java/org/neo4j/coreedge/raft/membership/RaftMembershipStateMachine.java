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
package org.neo4j.coreedge.raft.membership;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.state.FollowerStates;
import org.neo4j.helpers.Clock;
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
public class RaftMembershipStateMachine<MEMBER>
{
    private final Log log;
    public RaftMembershipStateMachineEventHandler<MEMBER> state = new Inactive();

    private final ReadableRaftLog raftLog;
    private final Clock clock;
    private final long electionTimeout;

    private final MembershipDriver<MEMBER> membershipDriver;
    private long catchupTimeout;
    private final RaftMembershipImpl<MEMBER> membershipState;

    private MEMBER catchingUpMember;

    public RaftMembershipStateMachine( ReadableRaftLog raftLog, Clock clock, long electionTimeout,
                                       MembershipDriver<MEMBER> membershipDriver, LogProvider logProvider,
                                       long catchupTimeout, RaftMembershipImpl<MEMBER> membershipState )
    {
        this.raftLog = raftLog;
        this.clock = clock;
        this.electionTimeout = electionTimeout;
        this.membershipDriver = membershipDriver;
        this.catchupTimeout = catchupTimeout;
        this.membershipState = membershipState;
        this.log = logProvider.getLog( getClass() );
    }

    private synchronized void handleState( RaftMembershipStateMachineEventHandler<MEMBER> newState )
    {
        RaftMembershipStateMachineEventHandler oldState = state;
        this.state = newState;

        if ( oldState != newState )
        {
            oldState.onExit();
            newState.onEntry();

            log.info( newState.toString() );
            membershipDriver.stateChanged();
        }
    }

    public void onRole( Role role )
    {
        handleState( state.onRole( role ) );
    }

    public void onRaftGroupCommitted()
    {
        handleState( state.onRaftGroupCommitted() );
    }

    public void onFollowerStateChange( FollowerStates<MEMBER> followerStates )
    {
        handleState( state.onFollowerStateChange( followerStates ) );
    }

    public void onMissingMember( MEMBER member )
    {
        handleState( state.onMissingMember( member ) );
    }

    public void onSuperfluousMember( MEMBER member )
    {
        handleState( state.onSuperfluousMember( member ) );
    }

    public void onTargetChanged( Set<MEMBER> targetMembers )
    {
        handleState( state.onTargetChanged( targetMembers ) );
    }

    public class Inactive extends RaftMembershipStateMachineEventHandler.Adapter<MEMBER>
    {
        @Override
        public RaftMembershipStateMachineEventHandler<MEMBER> onRole( Role role )
        {
            if ( role == Role.LEADER )
            {
                if ( membershipDriver.uncommittedMemberChangeInLog() )
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

    public abstract class ActiveBaseState extends RaftMembershipStateMachineEventHandler.Adapter<MEMBER>
    {
        @Override
        public RaftMembershipStateMachineEventHandler<MEMBER> onRole( Role role )
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

    public class Idle extends ActiveBaseState
    {
        @Override
        public RaftMembershipStateMachineEventHandler<MEMBER> onMissingMember( MEMBER member )
        {
            return new CatchingUp( member );
        }

        @Override
        public RaftMembershipStateMachineEventHandler<MEMBER> onSuperfluousMember( MEMBER member )
        {
            Set<MEMBER> updatedVotingMembers = new HashSet<>( membershipState.votingMembers() );
            updatedVotingMembers.remove( member );
            membershipDriver.doConsensus( updatedVotingMembers );

            return new ConsensusInProgress();
        }

        @Override
        public String toString()
        {
            return "Idle{}";
        }
    }

    public class CatchingUp extends ActiveBaseState
    {
        private final CatchupGoalTracker catchupGoalTracker;
        boolean movingToConsensus;

        public CatchingUp( MEMBER member )
        {
            this.catchupGoalTracker = new CatchupGoalTracker( raftLog, clock, electionTimeout, catchupTimeout );
            catchingUpMember = member;
        }

        @Override
        public void onEntry()
        {
            membershipState.addAdditionalReplicationMember( catchingUpMember );
        }

        @Override
        public void onExit()
        {
            if( !movingToConsensus )
            {
                membershipState.removeAdditionalReplicationMember( catchingUpMember );
            }
        }

        @Override
        public RaftMembershipStateMachineEventHandler<MEMBER> onRole( Role role )
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
        public RaftMembershipStateMachineEventHandler<MEMBER> onFollowerStateChange( FollowerStates<MEMBER> followerStates )
        {
            catchupGoalTracker.updateProgress( followerStates.get( catchingUpMember ) );

            if ( catchupGoalTracker.isFinished() )
            {
                if ( catchupGoalTracker.isGoalAchieved() )
                {
                    Set<MEMBER> updatedVotingMembers = new HashSet<>( membershipState.votingMembers() );
                    updatedVotingMembers.add( catchingUpMember );
                    membershipDriver.doConsensus( updatedVotingMembers );

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
        public RaftMembershipStateMachineEventHandler<MEMBER> onTargetChanged( Set<MEMBER> targetMembers )
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
        public RaftMembershipStateMachineEventHandler<MEMBER> onRaftGroupCommitted()
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
            membershipState.removeAdditionalReplicationMember( catchingUpMember );
        }

        @Override
        public String toString()
        {
            return "ConsensusInProgress{}";
        }
    }
}
