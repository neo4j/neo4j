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
package org.neo4j.causalclustering.load_balancing.plugins.server_policies;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.load_balancing.Endpoint;
import org.neo4j.causalclustering.load_balancing.LoadBalancingPlugin;
import org.neo4j.causalclustering.load_balancing.LoadBalancingResult;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyList;
import static org.neo4j.causalclustering.load_balancing.Util.asList;
import static org.neo4j.causalclustering.load_balancing.Util.extractBoltAddress;
import static org.neo4j.causalclustering.load_balancing.plugins.server_policies.FilteringPolicyLoader.load;

/**
 * The server policies plugin defines policies on the server-side which
 * can be bound to by a client by supplying a appropriately formed context.
 *
 * An example would be to define different policies for different regions.
 */
@Service.Implementation( LoadBalancingPlugin.class )
public class ServerPoliciesPlugin implements LoadBalancingPlugin
{
    public static final String PLUGIN_NAME = "server_policies";

    private TopologyService topologyService;
    private LeaderLocator leaderLocator;
    private Long timeToLive;
    private boolean allowReadsOnFollowers;
    private Policies policies;

    @Override
    public void validate( Config config, Log log ) throws InvalidSettingException
    {
        try
        {
            load( config, PLUGIN_NAME, log );
        }
        catch ( InvalidFilterSpecification e )
        {
            throw new InvalidSettingException( "Invalid filter specification", e );
        }
    }

    @Override
    public void init( TopologyService topologyService, LeaderLocator leaderLocator,
            LogProvider logProvider, Config config ) throws InvalidFilterSpecification
    {
        this.topologyService = topologyService;
        this.leaderLocator = leaderLocator;
        this.timeToLive = config.get( CausalClusteringSettings.cluster_routing_ttl );
        this.allowReadsOnFollowers = config.get( CausalClusteringSettings.cluster_allow_reads_on_followers );
        this.policies = load( config, PLUGIN_NAME, logProvider.getLog( getClass() ) );
    }

    @Override
    public String pluginName()
    {
        return PLUGIN_NAME;
    }

    @Override
    public Result run( Map<String,String> context ) throws ProcedureException
    {
        Policy policy = policies.selectFor( context );

        CoreTopology coreTopology = topologyService.coreServers();
        ReadReplicaTopology rrTopology = topologyService.readReplicas();

        return new LoadBalancingResult( routeEndpoints( coreTopology ), writeEndpoints( coreTopology ),
                readEndpoints( coreTopology, rrTopology, policy ), timeToLive );
    }

    private List<Endpoint> routeEndpoints( CoreTopology cores )
    {
        return cores.members().values().stream().map( extractBoltAddress() )
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

    private List<Endpoint> readEndpoints( CoreTopology coreTopology, ReadReplicaTopology rrTopology, Policy policy )
    {
        Set<ServerInfo> possibleReaders = rrTopology.members().entrySet().stream()
                .map( entry -> new ServerInfo( entry.getValue().connectors().boltAddress(), entry.getKey(),
                        entry.getValue().groups() ) )
                .collect( Collectors.toSet() );

        if ( allowReadsOnFollowers || possibleReaders.size() == 0 )
        {
            Set<MemberId> validCores = coreTopology.members().keySet();
            try
            {
                MemberId leader = leaderLocator.getLeader();
                validCores = validCores.stream().filter( memberId -> !memberId.equals( leader ) ).collect( Collectors.toSet() );
            }
            catch ( NoLeaderFoundException ignored )
            {
                // we might end up using the leader for reading during this ttl, should be fine in general
            }

            for ( MemberId validCore : validCores )
            {
                Optional<CoreServerInfo> coreServerInfo = coreTopology.find( validCore );
                if ( coreServerInfo.isPresent() )
                {
                    CoreServerInfo serverInfo = coreServerInfo.get();
                    possibleReaders.add(
                            new ServerInfo( serverInfo.connectors().boltAddress(), validCore, serverInfo.groups() ) );
                }
            }
        }

        Set<ServerInfo> readers = policy.apply( possibleReaders );
        return readers.stream().map( r -> Endpoint.read( r.boltAddress() ) ).collect( Collectors.toList() );
    }
}
