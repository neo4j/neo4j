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
package org.neo4j.causalclustering.readreplica;

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
import org.neo4j.causalclustering.load_balancing.filters.Filter;
import org.neo4j.causalclustering.load_balancing.plugins.server_policies.FilterConfigParser;
import org.neo4j.causalclustering.load_balancing.plugins.server_policies.InvalidFilterSpecification;
import org.neo4j.causalclustering.load_balancing.plugins.server_policies.ServerInfo;
import org.neo4j.helpers.Service;

@Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
public class UserDefinedConfigurationStrategy extends UpstreamDatabaseSelectionStrategy
{
    public UserDefinedConfigurationStrategy()
    {
        super( "user-defined" );
    }

    @Override
    public Optional<MemberId> upstreamDatabase() throws UpstreamDatabaseSelectionException
    {
        try
        {
            Filter<ServerInfo> filters = FilterConfigParser.parse( config.get( CausalClusteringSettings.user_defined_upstream_selection_strategy ) );

            Set<ServerInfo> possibleServers = possibleServers();

            return filters.apply( possibleServers ).stream()
                    .map( ServerInfo::memberId )
                    .filter( memberId -> !Objects.equals( myself, memberId ) )
                    .findFirst();
        }
        catch ( InvalidFilterSpecification invalidFilterSpecification )
        {
            return Optional.empty();
        }
    }

    private Set<ServerInfo> possibleServers()
    {
        Stream<Map.Entry<MemberId, ? extends DiscoveryServerInfo>> infoMap =
                Stream.of( topologyService.readReplicas(), topologyService.coreServers() )
                        .map( Topology::members )
                        .map( Map::entrySet )
                        .flatMap( Set::stream );

        return infoMap
                .map( this::toServerInfo )
                .collect( Collectors.toSet() );
    }

    private <T extends DiscoveryServerInfo> ServerInfo toServerInfo( Map.Entry<MemberId, T> entry )
    {
        T server = entry.getValue();
        MemberId memberId = entry.getKey();
        return new ServerInfo( server.connectors().boltAddress(), memberId, server.groups() );
    }
}
