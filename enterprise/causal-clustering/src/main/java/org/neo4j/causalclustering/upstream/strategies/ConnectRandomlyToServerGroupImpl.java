/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.upstream.strategies;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;

public class ConnectRandomlyToServerGroupImpl
{
    private final List<String> groups;
    private final TopologyService topologyService;
    private final MemberId myself;
    private final Random random = new Random();

    ConnectRandomlyToServerGroupImpl( List<String> groups, TopologyService topologyService, MemberId myself )
    {
        this.groups = groups;
        this.topologyService = topologyService;
        this.myself = myself;
    }

    public Optional<MemberId> upstreamDatabase()
    {
        Map<MemberId,ReadReplicaInfo> replicas = topologyService.localReadReplicas().members();

        List<MemberId> choices =
                groups.stream().flatMap( group -> replicas.entrySet().stream().filter( isMyGroupAndNotMe( group ) ) ).map( Map.Entry::getKey ).collect(
                        Collectors.toList() );

        if ( choices.isEmpty() )
        {
            return Optional.empty();
        }
        else
        {
            return Optional.of( choices.get( random.nextInt( choices.size() ) ) );
        }
    }

    private Predicate<Map.Entry<MemberId,ReadReplicaInfo>> isMyGroupAndNotMe( String group )
    {
        return entry -> entry.getValue().groups().contains( group ) && !entry.getKey().equals( myself );
    }
}
