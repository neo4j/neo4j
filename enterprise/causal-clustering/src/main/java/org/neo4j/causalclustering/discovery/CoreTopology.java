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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

public class CoreTopology
{
    static CoreTopology EMPTY = new CoreTopology( null, false, Collections.emptyMap() );

    private final ClusterId clusterId;
    private final boolean canBeBootstrapped;
    private final Map<MemberId,CoreAddresses> coreMembers;

    public CoreTopology( ClusterId clusterId, boolean canBeBootstrapped, Map<MemberId,CoreAddresses> coreMembers )
    {
        this.clusterId = clusterId;
        this.canBeBootstrapped = canBeBootstrapped;
        this.coreMembers = new HashMap<>( coreMembers );
    }

    public Set<MemberId> members()
    {
        return coreMembers.keySet();
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public Collection<CoreAddresses> addresses()
    {
        return coreMembers.values();
    }

    public boolean canBeBootstrapped()
    {
        return canBeBootstrapped;
    }

    public Optional<CoreAddresses> find( MemberId memberId )
    {
        return Optional.ofNullable( coreMembers.get( memberId ) );
    }

    @Override
    public String toString()
    {
        return format( "{clusterId=%s, bootstrappable=%s, coreMembers=%s}", clusterId, canBeBootstrapped(),
                coreMembers );
    }

    TopologyDifference difference( CoreTopology other )
    {
        Set<MemberId> members = coreMembers.keySet();
        Set<MemberId> otherMembers = other.coreMembers.keySet();

        Set<Difference> added = otherMembers.stream().filter( m -> !members.contains( m ) )
                .map( memberId -> asDifference( other, memberId ) ).collect( toSet() );

        Set<Difference> removed = members.stream().filter( m -> !otherMembers.contains( m ) )
                .map( memberId -> asDifference( CoreTopology.this, memberId ) ).collect( toSet() );

        return new TopologyDifference( added, removed );
    }

    private Difference asDifference( CoreTopology topology, MemberId memberId )
    {
        return new Difference( memberId, topology.find( memberId ).orElse( null ) );
    }

    public Optional<MemberId> anyCoreMemberId()
    {
        if ( coreMembers.keySet().size() == 0 )
        {
            return Optional.empty();
        }
        else
        {
            return coreMembers.keySet().stream().findAny();
        }
    }

    class TopologyDifference
    {
        private Set<Difference> added;
        private Set<Difference> removed;

        TopologyDifference( Set<Difference> added, Set<Difference> removed )
        {
            this.added = added;
            this.removed = removed;
        }

        Set<Difference> added()
        {
            return added;
        }

        Set<Difference> removed()
        {
            return removed;
        }

        boolean hasChanges()
        {
            return added.size() > 0 || removed.size() > 0;
        }

        @Override
        public String toString()
        {
            return String.format( "{added=%s, removed=%s}", added, removed );
        }
    }

    private class Difference
    {
        private MemberId memberId;
        private CoreAddresses coreAddresses;

        Difference( MemberId memberId, CoreAddresses coreAddresses )
        {
            this.memberId = memberId;
            this.coreAddresses = coreAddresses;
        }

        @Override
        public String toString()
        {
            return String.format( "{memberId=%s, coreAddresses=%s}", memberId, coreAddresses );
        }
    }
}
