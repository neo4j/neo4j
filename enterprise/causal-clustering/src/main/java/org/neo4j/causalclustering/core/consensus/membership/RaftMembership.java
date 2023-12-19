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
package org.neo4j.causalclustering.core.consensus.membership;

import java.util.Set;

import org.neo4j.causalclustering.identity.MemberId;

/**
 * Exposes a view of the members of a Raft cluster. Essentially it gives access to two sets - the set of voting
 * members and the set of replication members.
 * This class also allows for listeners to be notified of membership changes.
 */
public interface RaftMembership
{
    /**
     * @return members whose votes count towards consensus. The returned set should be considered immutable.
     */
    Set<MemberId> votingMembers();

    /**
     * @return members to which replication should be attempted. The returned set should be considered immutable.
     */
    Set<MemberId> replicationMembers();

    /**
     * Register a membership listener.
     *
     * @param listener The listener.
     */
    void registerListener( RaftMembership.Listener listener );

    /**
     * This interface must be implemented from whoever wants to be notified of membership changes. Membership changes
     * are additions to and removals from the voting and replication members set.
     */
    interface Listener
    {
        /**
         * This method is called on additions to and removals from either the voting or replication members sets.
         * The implementation has the responsibility of figuring out what the actual change is.
         */
        void onMembershipChanged();
    }
}
