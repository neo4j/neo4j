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
package org.neo4j.causalclustering.discovery;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toSet;

public class CoreTopology
{
    static CoreTopology EMPTY = new CoreTopology( null, false, emptyMap() );

    private final ClusterId clusterId;
    private final boolean canBeBootstrapped;
    private final Map<MemberId,CoreServerInfo> coreMembers;

    public CoreTopology( ClusterId clusterId, boolean canBeBootstrapped, Map<MemberId,CoreServerInfo> coreMembers )
    {
        this.clusterId = clusterId;
        this.canBeBootstrapped = canBeBootstrapped;
        this.coreMembers = new HashMap<>( coreMembers );
    }

    public Map<MemberId,CoreServerInfo> members()
    {
        return coreMembers;
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public boolean canBeBootstrapped()
    {
        return canBeBootstrapped;
    }

    public Optional<CoreServerInfo> find( MemberId memberId )
    {
        return Optional.ofNullable( coreMembers.get( memberId ) );
    }

    @Override
    public String toString()
    {
        return format( "{clusterId=%s, bootstrappable=%s, coreMembers=%s}", clusterId, canBeBootstrapped(), coreMembers );
    }

    TopologyDifference difference( CoreTopology other )
    {
        Set<MemberId> members = coreMembers.keySet();
        Set<MemberId> otherMembers = other.coreMembers.keySet();

        Set<Difference> added = otherMembers.stream().filter( m -> !members.contains( m ) )
                .map( memberId -> Difference.asDifference( other, memberId ) ).collect( toSet() );

        Set<Difference> removed = members.stream().filter( m -> !otherMembers.contains( m ) )
                .map( memberId -> Difference.asDifference( CoreTopology.this, memberId ) ).collect( toSet() );

        return new TopologyDifference( added, removed );
    }

    public Optional<MemberId> anyCoreMemberId()
    {
            return coreMembers.keySet().stream().findAny();
    }

}
