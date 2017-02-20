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
package org.neo4j.causalclustering.load_balancing.plugins;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.load_balancing.Endpoint;
import org.neo4j.causalclustering.load_balancing.LoadBalancingResult;
import org.neo4j.causalclustering.load_balancing.LoadBalancingPlugin;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyList;
import static java.util.stream.Stream.concat;
import static org.neo4j.causalclustering.load_balancing.Util.extractBoltAddress;
import static org.neo4j.causalclustering.load_balancing.Util.asList;

/**
 * This is just a simple plugin and not intended for actual use. Will be replaced.
 */
public class AllServersPlugin implements LoadBalancingPlugin
{
    private TopologyService topologyService;
    private LeaderLocator leaderLocator;
    private Long timeToLive;

    @Override
    public void init( TopologyService topologyService, LeaderLocator leaderLocator, LogProvider logProvider, Config config )
    {
        this.topologyService = topologyService;
        this.leaderLocator = leaderLocator;
        this.timeToLive = config.get( CausalClusteringSettings.cluster_routing_ttl );
    }

    @Override
    public String pluginName()
    {
        return "all_servers";
    }

    @Override
    public Result run( Map<String,String> context )
    {
        CoreTopology cores = topologyService.coreServers();
        ReadReplicaTopology readers = topologyService.readReplicas();

        return new LoadBalancingResult( routeEndpoints( cores ), writeEndpoints( cores ),
                readEndpoints( cores, readers ), timeToLive );
    }

    private List<Endpoint> routeEndpoints( CoreTopology cores )
    {
        return cores.allMemberInfo().stream().map( extractBoltAddress() )
                .map( Endpoint::route ).collect( Collectors.toList() );
    }

    private List<Endpoint> writeEndpoints( CoreTopology cores )
    {
        MemberId leader;
        try
        {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            return emptyList();
        }

        Optional<Endpoint> endPoint = cores.find( leader )
                .map( extractBoltAddress() )
                .map( Endpoint::write );

        return asList( endPoint );
    }

    private List<Endpoint> readEndpoints( CoreTopology cores, ReadReplicaTopology readers )
    {
        return concat( readers.allMemberInfo().stream(), cores.allMemberInfo().stream() )
                .map( extractBoltAddress() )
                .map( Endpoint::read )
                .collect( Collectors.toList() );
    }
}
