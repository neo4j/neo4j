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

import org.junit.Test;

import java.util.Map;

import org.neo4j.causalclustering.routing.load_balancing.filters.Filter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.load_balancing_config;
import static org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.FilterBuilder.filter;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class FilteringPolicyLoaderTest
{
    @Test
    public void shouldLoadConfiguredPolicies() throws Exception
    {
        // given
        String pluginName = "server_policies";

        Object[][] input = {
                {
                        "asia_west",

                        "groups(asia_west) -> min(2);" +
                        "groups(asia) -> min(2);",

                        filter().groups( "asia_west" ).min( 2 ).newRule()
                                .groups( "asia" ).min( 2 ).newRule()
                                .all() // implicit
                                .build()
                },
                {
                        "asia_east",

                        "groups(asia_east) -> min(2);" +
                        "groups(asia) -> min(2);",

                        filter().groups( "asia_east" ).min( 2 ).newRule()
                                .groups( "asia" ).min( 2 ).newRule()
                                .all() // implicit
                                .build()
                },
                {
                        "asia_only",

                        "groups(asia);" +
                        "halt();",

                        filter().groups( "asia" ).build()
                },
        };

        Config config = Config.defaults();

        for ( Object[] row : input )
        {
            String policyName = (String) row[0];
            String filterSpec = (String) row[1];
            config.augment( configNameFor( pluginName, policyName ), filterSpec );
        }

        // when
        Policies policies = FilteringPolicyLoader.load( config, pluginName, mock( Log.class ) );

        // then
        for ( Object[] row : input )
        {
            String policyName = (String) row[0];
            Policy loadedPolicy = policies.selectFor( policyNameContext( policyName ) );
            @SuppressWarnings( "unchecked" )
            Policy expectedPolicy = new FilteringPolicy( (Filter<ServerInfo>) row[2] );
            assertEquals( expectedPolicy, loadedPolicy );
        }
    }

    private static Map<String,String> policyNameContext( String policyName )
    {
        return stringMap( Policies.POLICY_KEY, policyName );
    }

    private static String configNameFor( String pluginName, String policyName )
    {
        return format( "%s.%s.%s", load_balancing_config.name(), pluginName, policyName );
    }
}
