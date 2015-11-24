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

import java.util.Set;

import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.state.FollowerStates;

interface RaftMembershipStateMachineEventHandler<MEMBER>
{
    RaftMembershipStateMachineEventHandler<MEMBER> onRole( Role role );

    RaftMembershipStateMachineEventHandler<MEMBER> onRaftGroupCommitted();

    RaftMembershipStateMachineEventHandler<MEMBER> onFollowerStateChange( FollowerStates<MEMBER> followerStates );

    RaftMembershipStateMachineEventHandler<MEMBER> onMissingMember( MEMBER member );

    RaftMembershipStateMachineEventHandler<MEMBER> onSuperfluousMember( MEMBER member );

    RaftMembershipStateMachineEventHandler<MEMBER> onTargetChanged( Set<MEMBER> targetMembers );

    void onExit();

    void onEntry();

    abstract class Adapter<MEMBER> implements RaftMembershipStateMachineEventHandler<MEMBER>
    {
        @Override
        public RaftMembershipStateMachineEventHandler<MEMBER> onRole( Role role )
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler<MEMBER> onRaftGroupCommitted()
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler<MEMBER> onMissingMember( MEMBER member )
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler<MEMBER> onSuperfluousMember( MEMBER member )
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler<MEMBER> onFollowerStateChange( FollowerStates<MEMBER> followerStates )
        {
            return this;
        }

        @Override
        public RaftMembershipStateMachineEventHandler<MEMBER> onTargetChanged( Set<MEMBER> targetMembers )
        {
            return this;
        }

        @Override
        public void onExit() {};

        @Override
        public void onEntry() {};
    }
}
