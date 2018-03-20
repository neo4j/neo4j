/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.routing.load_balancing;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.routing.load_balancing.plugins.ServerShufflingProcessor;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Loads and initialises any service implementations of <class>LoadBalancingPlugin</class>.
 * Exposes configured instances of that interface via an iterator.
 */
public class LoadBalancingPluginLoader
{
    private LoadBalancingPluginLoader()
    {
    }

    public static void validate( Config config, Log log ) throws InvalidSettingException
    {
        LoadBalancingPlugin plugin = findPlugin( config );
        plugin.validate( config, log );
    }

    public static LoadBalancingProcessor load( TopologyService topologyService, LeaderLocator leaderLocator,
            LogProvider logProvider, Config config ) throws Throwable
    {
        LoadBalancingPlugin plugin = findPlugin( config );
        plugin.init( topologyService, leaderLocator, logProvider, config );

        if ( config.get( CausalClusteringSettings.load_balancing_shuffle ) )
        {
            return new ServerShufflingProcessor( plugin );
        }

        return plugin;
    }

    private static LoadBalancingPlugin findPlugin( Config config ) throws InvalidSettingException
    {
        Set<String> availableOptions = new HashSet<>();
        Iterable<LoadBalancingPlugin> allImplementationsOnClasspath = Service.load( LoadBalancingPlugin.class );

        String configuredName = config.get( CausalClusteringSettings.load_balancing_plugin );

        for ( LoadBalancingPlugin plugin : allImplementationsOnClasspath )
        {
            if ( plugin.pluginName().equals( configuredName ) )
            {
                return plugin;
            }
            availableOptions.add( plugin.pluginName() );
        }

        throw new InvalidSettingException( String.format( "Could not find load balancing plugin with name: '%s'" +
                                                   " among available options: %s", configuredName, availableOptions ) );
    }
}
