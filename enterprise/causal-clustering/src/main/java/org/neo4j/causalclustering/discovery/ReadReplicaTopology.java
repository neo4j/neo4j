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
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.neo4j.causalclustering.identity.MemberId;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toSet;


public class ReadReplicaTopology
{
    static final ReadReplicaTopology EMPTY = new ReadReplicaTopology( emptyMap() );

    private final Map<MemberId,ReadReplicaInfo> readReplicaMembers;

    public ReadReplicaTopology( Map<MemberId,ReadReplicaInfo> readReplicaMembers )
    {
        this.readReplicaMembers = readReplicaMembers;
    }

    public Collection<ReadReplicaInfo> allMemberInfo()
    {
        return readReplicaMembers.values();
    }

    public Map<MemberId,ReadReplicaInfo> members()
    {
        return readReplicaMembers;
    }

    Optional<ReadReplicaInfo> find( MemberId memberId )
    {
        return Optional.ofNullable( readReplicaMembers.get( memberId ) );
    }

    @Override
    public String toString()
    {
        return String.format( "{readReplicas=%s}", readReplicaMembers );
    }

    public Optional<MemberId> anyReadReplicaMemberId()
    {
        if ( readReplicaMembers.keySet().size() == 0 )
        {
            return Optional.empty();
        }
        else
        {
            return readReplicaMembers.keySet().stream().findAny();
        }
    }

    ReadReplicaTopologyDifference difference( ReadReplicaTopology other )
    {
        Set<MemberId> members = readReplicaMembers.keySet();
        Set<MemberId> otherMembers = other.readReplicaMembers.keySet();

        Set<ReadReplicaTopology.Difference> added = otherMembers.stream().filter( m -> !members.contains( m ) )
                .map( memberId -> asDifference( other, memberId ) ).collect( toSet() );

        Set<ReadReplicaTopology.Difference> removed = members.stream().filter( m -> !otherMembers.contains( m ) )
                .map( memberId -> asDifference( ReadReplicaTopology.this, memberId ) ).collect( toSet() );

        return new ReadReplicaTopologyDifference( added, removed );
    }

    private ReadReplicaTopology.Difference asDifference( ReadReplicaTopology topology, MemberId memberId )
    {
        return new ReadReplicaTopology.Difference( memberId, topology.find( memberId ).orElse( null ) );
    }

    class ReadReplicaTopologyDifference
    {
        private Set<ReadReplicaTopology.Difference> added;
        private Set<ReadReplicaTopology.Difference> removed;

        ReadReplicaTopologyDifference( Set<ReadReplicaTopology.Difference> added, Set<ReadReplicaTopology.Difference> removed )
        {
            this.added = added;
            this.removed = removed;
        }

        Set<ReadReplicaTopology.Difference> added()
        {
            return added;
        }

        Set<ReadReplicaTopology.Difference> removed()
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
        private ReadReplicaInfo readReplicaInfo;

        Difference( MemberId memberId, ReadReplicaInfo readReplicaInfo )
        {
            this.memberId = memberId;
            this.readReplicaInfo = readReplicaInfo;
        }

        @Override
        public String toString()
        {
            return String.format( "{memberId=%s, readReplicaInfo=%s}", memberId, readReplicaInfo );
        }
    }
}
