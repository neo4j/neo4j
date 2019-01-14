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
