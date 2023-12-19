/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.upstream.strategies;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import org.neo4j.causalclustering.discovery.Topology;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.routing.load_balancing.filters.Filter;
import org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.FilterConfigParser;
import org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.InvalidFilterSpecification;
import org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.ServerInfo;
import org.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;
import org.neo4j.helpers.Service;

@Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
public class UserDefinedConfigurationStrategy extends UpstreamDatabaseSelectionStrategy
{

    public static final String IDENTITY = "user-defined";
    // Empty if provided filter config cannot be parsed.
    // Ideally this class would not be created until config has been successfully parsed
    // in which case there would be no need for Optional
    private Optional<Filter<ServerInfo>> filters;

    public UserDefinedConfigurationStrategy()
    {
        super( IDENTITY );
    }

    @Override
    public void init()
    {
        String filterConfig = config.get( CausalClusteringSettings.user_defined_upstream_selection_strategy );
        try
        {
            Filter<ServerInfo> parsed = FilterConfigParser.parse( filterConfig );
            filters = Optional.of( parsed );
            log.info( "Upstream selection strategy " + readableName + " configured with " + filterConfig );
        }
        catch ( InvalidFilterSpecification invalidFilterSpecification )
        {
            filters = Optional.empty();
            log.warn( "Cannot parse configuration '" + filterConfig + "' for upstream selection strategy " + readableName + ". " +
                    invalidFilterSpecification.getMessage() );
        }
    }

    @Override
    public Optional<MemberId> upstreamDatabase()
    {
        return filters.flatMap( filters ->
        {
            Set<ServerInfo> possibleServers = possibleServers();

            return filters.apply( possibleServers ).stream().map( ServerInfo::memberId ).filter( memberId -> !Objects.equals( myself, memberId ) ).findFirst();
        } );
    }

    private Set<ServerInfo> possibleServers()
    {
        Stream<Map.Entry<MemberId,? extends DiscoveryServerInfo>> infoMap =
                Stream.of( topologyService.localReadReplicas(), topologyService.localCoreServers() ).map( Topology::members ).map( Map::entrySet ).flatMap(
                        Set::stream );

        return infoMap.map( this::toServerInfo ).collect( Collectors.toSet() );
    }

    private <T extends DiscoveryServerInfo> ServerInfo toServerInfo( Map.Entry<MemberId,T> entry )
    {
        T server = entry.getValue();
        MemberId memberId = entry.getKey();
        return new ServerInfo( server.connectors().boltAddress(), memberId, server.groups() );
    }
}
