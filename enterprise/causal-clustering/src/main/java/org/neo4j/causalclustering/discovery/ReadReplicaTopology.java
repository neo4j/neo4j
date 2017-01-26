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
import java.util.stream.Collectors;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.collection.Pair;

import static java.util.Collections.emptyMap;


public class ReadReplicaTopology
{
    public static final ReadReplicaTopology EMPTY = new ReadReplicaTopology( emptyMap() );

    private final Map<MemberId,ReadReplicaAddresses> readReplicaMembers;

    public ReadReplicaTopology( Map<MemberId,ReadReplicaAddresses> readReplicaMembers )
    {
        this.readReplicaMembers = readReplicaMembers;
    }

    public Collection<ReadReplicaAddresses> members()
    {
        return readReplicaMembers.values();
    }

    public Optional<ReadReplicaAddresses> find( MemberId memberId )
    {
        return Optional.ofNullable( readReplicaMembers.get( memberId ) );
    }

//    public Set<ReadReplicaAddresses> difference( ReadReplicaTopology other )
//    {
//        Pair<Set<ReadReplicaAddresses>, Set<ReadReplicaAddresses>> split = split( readReplicaMembers, other.members() );
//        Set<ReadReplicaAddresses> big = split.first();
//        Set<ReadReplicaAddresses> small = split.other();
//
//        return big.stream().filter( n -> !small.contains( n ) ).collect( Collectors.toSet() );
//    }

    private Pair<Set<ReadReplicaAddresses>, Set<ReadReplicaAddresses>> split(
            Set<ReadReplicaAddresses> one, Set<ReadReplicaAddresses> two )
    {
        if ( one.size() > two.size() )
        {
            return Pair.pair( one, two );
        }
        else
        {
            return Pair.pair( two, one );
        }
    }

    @Override
    public String toString()
    {
        return String.format( "{readReplicas=%s}", readReplicaMembers );
    }
}
