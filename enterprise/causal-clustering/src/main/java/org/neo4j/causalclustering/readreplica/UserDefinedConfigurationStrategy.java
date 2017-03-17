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

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
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
            Filter<ServerInfo> filters = FilterConfigParser
                    .parse( config.get( CausalClusteringSettings.user_defined_upstream_selection_strategy ) );

            Set<ServerInfo> possibleReaders = topologyService.readReplicas().members().entrySet().stream()
                    .map( entry -> new ServerInfo( entry.getValue().connectors().boltAddress(), entry.getKey(),
                            entry.getValue().groups() ) ).collect( Collectors.toSet() );

            CoreTopology coreTopology = topologyService.coreServers();
            for ( MemberId validCore : coreTopology.members().keySet() )
            {
                Optional<CoreServerInfo> coreServerInfo = coreTopology.find( validCore );
                if ( coreServerInfo.isPresent() )
                {
                    CoreServerInfo serverInfo = coreServerInfo.get();
                    possibleReaders.add(
                            new ServerInfo( serverInfo.connectors().boltAddress(), validCore, serverInfo.groups() ) );
                }
            }

            return filters.apply( possibleReaders ).stream().map( ServerInfo::memberId ).findAny();
        }
        catch ( InvalidFilterSpecification invalidFilterSpecification )
        {
            return Optional.empty();
        }
    }
}
