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
package org.neo4j.causalclustering.load_balancing.strategy.server_policy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.load_balancing.Endpoint;
import org.neo4j.causalclustering.load_balancing.LoadBalancingResult;
import org.neo4j.causalclustering.load_balancing.LoadBalancingStrategy;
import org.neo4j.causalclustering.load_balancing.filters.Filter;
import org.neo4j.kernel.configuration.Config;

import static java.util.Collections.emptyList;
import static org.neo4j.causalclustering.load_balancing.Util.asList;
import static org.neo4j.causalclustering.load_balancing.Util.extractBoltAddress;

// TODO: This is work in progress. Currently mostly copies V1 behaviour.
public class ServerPolicyStrategy implements LoadBalancingStrategy
{
    private final CoreTopologyService topologyService;
    private final LeaderLocator leaderLocator;
    private final Long timeToLive;
    private final boolean allowReadsOnFollowers;
    private final Policies policies;

    public ServerPolicyStrategy( CoreTopologyService topologyService,
            LeaderLocator leaderLocator, Policies policies, Config config )
    {
        this.topologyService = topologyService;
        this.leaderLocator = leaderLocator;
        this.timeToLive = config.get( CausalClusteringSettings.cluster_routing_ttl );
        this.allowReadsOnFollowers = config.get( CausalClusteringSettings.cluster_allow_reads_on_followers );
        this.policies = policies;
    }

    @Override
    public Result run( Map<String,String> context )
    {
        CoreTopology coreTopology = topologyService.coreServers();
        ReadReplicaTopology rrTopology = topologyService.readReplicas();

        return new LoadBalancingResult( routeEndpoints( coreTopology ), writeEndpoints( coreTopology ),
                readEndpoints( coreTopology, rrTopology, policies.selectFor( context ) ), timeToLive );
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

    private List<Endpoint> readEndpoints( CoreTopology coreTopology, ReadReplicaTopology rrTopology, Filter<ServerInfo> policyFilter )
    {
        Set<ServerInfo> possibleReaders = rrTopology.allMemberInfo().stream()
                .map( info -> new ServerInfo( info.connectors().boltAddress(), info.tags() ) )
                .collect( Collectors.toSet() );

        if ( allowReadsOnFollowers || possibleReaders.size() == 0 )
        {
            Set<MemberId> validCores = coreTopology.members();
            try
            {
                MemberId leader = leaderLocator.getLeader();
                validCores = validCores.stream().filter( memberId -> !memberId.equals( leader ) ).collect( Collectors.toSet() );
            }
            catch ( NoLeaderFoundException ignored )
            {
                // we might end up using the leader for reading during this ttl, should be fine in general
            }

            possibleReaders.addAll( validCores.stream().map( coreTopology::find ).map( Optional::get )
                    .map( info -> new ServerInfo( info.connectors().boltAddress(), info.tags() ) )
                    .collect( Collectors.toSet() ) );
        }

        Set<ServerInfo> readers = policyFilter.apply( possibleReaders );
        return readers.stream().map( r -> Endpoint.read( r.boltAddress() ) ).collect( Collectors.toList() );
    }
}
