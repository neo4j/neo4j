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
package org.neo4j.causalclustering.discovery;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.identity.MemberId;

import static java.util.stream.Collectors.toSet;

public interface Topology<T extends DiscoveryServerInfo>
{
    Map<MemberId, T> members();

    default TopologyDifference difference( Topology<T> other )
    {
        Set<MemberId> members = members().keySet();
        Set<MemberId> otherMembers = other.members().keySet();

        Set<Difference> added = otherMembers.stream().filter( m -> !members.contains( m ) )
                .map( memberId -> Difference.asDifference( other, memberId ) ).collect( toSet() );

        Set<Difference> removed = members.stream().filter( m -> !otherMembers.contains( m ) )
                .map( memberId -> Difference.asDifference( this, memberId ) ).collect( toSet() );

        return new TopologyDifference( added, removed );
    }

    default Optional<T> find( MemberId memberId )
    {
        return Optional.ofNullable( members().get( memberId ) );
    }

    default Map<MemberId, T> filterHostsByDb( Map<MemberId, T> s, String dbName )
    {
        return s.entrySet().stream().filter(e -> e.getValue().getDatabaseName().equals( dbName ) )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
    }

    Topology<T> filterTopologyByDb( String dbName );
}
