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
package org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.routing.load_balancing.filters.Filter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;

import static java.lang.String.format;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.load_balancing_config;

/**
 * Loads filters under the name space of a particular plugin.
 */
class FilteringPolicyLoader
{
    private FilteringPolicyLoader()
    {
    }

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
