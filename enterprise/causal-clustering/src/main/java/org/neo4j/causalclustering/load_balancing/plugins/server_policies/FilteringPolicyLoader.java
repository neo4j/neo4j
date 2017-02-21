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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.load_balancing.filters.Filter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.load_balancing_config;

/**
 * Loads filters under the name space of a particular plugin.
 */
class FilteringPolicyLoader
{
    static Policies load( Config config, String pluginName, Log log ) throws InvalidFilterSpecification
    {
        Policies policies = new Policies( log );

        String prefix = policyPrefix( pluginName );
        Map<String,String> rawConfig = config.getRaw();

        Set<String> configKeys = rawConfig.keySet().stream()
                .filter( e -> e.startsWith( prefix ) )
                .collect( Collectors.toSet() );

        for ( String configKey : configKeys )
        {
            String policyName = configKey.substring( prefix.length() );
            String filterSpec = rawConfig.get( configKey );

            Filter<ServerInfo> filter = FilterConfigParser.parse( filterSpec );
            policies.addPolicy( policyName, new FilteringPolicy( filter ) );
        }

        return policies;
    }

    private static String policyPrefix( String pluginName )
    {
        return format( "%s.%s.", load_balancing_config.name(), pluginName );
    }
}
