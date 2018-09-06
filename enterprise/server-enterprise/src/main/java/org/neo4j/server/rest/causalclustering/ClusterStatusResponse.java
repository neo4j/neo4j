/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.server.rest.causalclustering;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.identity.MemberId;

@JsonSerialize( include = JsonSerialize.Inclusion.NON_NULL )
public class ClusterStatusResponse
{
    private final boolean isCore;
    private final long lastAppliedRaftIndex;
    private final boolean isParticipatingInRaftGroup;
    private final Collection<String> votingMembers;
    private final boolean isHealthy;
    private final String memberId;
    private final String leader;
    private final Long millisSinceLastLeaderMessage;

    ClusterStatusResponse( long lastAppliedRaftIndex, boolean isParticipatingInRaftGroup, Collection<MemberId> votingMembers, boolean isHealthy,
            MemberId memberId, MemberId leader, Duration millisSinceLastLeaderMessage, boolean isCore )
    {
        this.lastAppliedRaftIndex = lastAppliedRaftIndex;
        this.isParticipatingInRaftGroup = isParticipatingInRaftGroup;
        this.votingMembers = votingMembers.stream().map( member -> member.getUuid().toString() ).sorted().collect( Collectors.toList() );
        this.isHealthy = isHealthy;
        this.memberId = memberId.getUuid().toString();
        this.leader = Optional.ofNullable( leader ).map( MemberId::getUuid ).map( UUID::toString ).orElse( null );
        this.millisSinceLastLeaderMessage = Optional.ofNullable( millisSinceLastLeaderMessage ).map( Duration::toMillis ).orElse( null );
        this.isCore = isCore;
    }

    /**
     * Transactions are associated with raft log indexes. By tracking this value across a cluster you will be able to evaluate with whether
     * the cluster is caught up and functioning as expected.
     *
     * @return the latest transaction id available on this node
     */
    public long getLastAppliedRaftIndex()
    {
        return lastAppliedRaftIndex;
    }

    /**
     * A node is considered participating if it believes it is caught up and knows who the leader is. Leader timeouts will prevent this value from being true
     * even if the core is caught up. This is always false for replicas, since they never participate in raft. The refuse to be leader flag does not affect this
     * logic (i.e. if a core proposes itself to be leader, it still doesn't know who the leader is since it the leader has not been voted in)
     *
     * @return true if the core is in a "good state" (up to date and part of raft). For cores this is likely the flag you will want to look at
     */
    public boolean isParticipatingInRaftGroup()
    {
        return isParticipatingInRaftGroup;
    }

    /**
     * For cores, this will list all known live core members. Read replicas also include all known read replicas.
     * Users will want to monitor this field (size or values) when performing rolling upgrades for read replicas.
     *
     * @return a list of discovery addresses ("hostname:port") that are part of this node's membership set
     */
    public Collection<String> getVotingMembers()
    {
        return votingMembers;
    }

    public boolean isHealthy()
    {
        return isHealthy;
    }

    public String getMemberId()
    {
        return memberId;
    }

    public String getLeader()
    {
        return leader;
    }

    public Long getMillisSinceLastLeaderMessage()
    {
        return millisSinceLastLeaderMessage;
    }

    public boolean isCore()
    {
        return isCore;
    }
}
