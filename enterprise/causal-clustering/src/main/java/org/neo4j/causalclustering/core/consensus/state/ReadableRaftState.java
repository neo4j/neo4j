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

import java.util.Set;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import org.neo4j.causalclustering.identity.MemberId;

public interface ReadableRaftState
{
    MemberId myself();

    Set<MemberId> votingMembers();

    Set<MemberId> replicationMembers();

    long term();

    MemberId leader();

    LeaderInfo leaderInfo();

    long leaderCommit();

    MemberId votedFor();

    Set<MemberId> votesForMe();

    Set<MemberId> heartbeatResponses();

    long lastLogIndexBeforeWeBecameLeader();

    FollowerStates<MemberId> followerStates();

    ReadableRaftLog entryLog();

    long commitIndex();

    boolean supportPreVoting();

    boolean isPreElection();

    Set<MemberId> preVotesForMe();

    boolean refusesToBeLeader();
}
