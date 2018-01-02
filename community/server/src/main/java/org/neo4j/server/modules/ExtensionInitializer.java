/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.modules;

import java.util.Collection;
import java.util.HashSet;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ConfigWrappingConfiguration;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.server.plugins.PluginLifecycle;
import org.neo4j.server.plugins.SPIPluginLifecycle;

/**
 * Allows unmanaged extensions to provide their own initialization
 */
public class ExtensionInitializer
{
    private final Iterable<PluginLifecycle> lifecycles;
    private final NeoServer neoServer;

    public ExtensionInitializer( NeoServer neoServer )
    {
        this.neoServer = neoServer;
        lifecycles = Service.load( PluginLifecycle.class );
    }

    public Collection<Injectable<?>> initializePackages( Iterable<String> packageNames )
    {
        GraphDatabaseAPI graphDatabaseService = neoServer.getDatabase().getGraph();
        Config configuration = neoServer.getConfig();

        Collection<Injectable<?>> injectables = new HashSet<>();
        for ( PluginLifecycle lifecycle : lifecycles )
        {
            if ( hasPackage( lifecycle, packageNames ) )
            {
                if ( lifecycle instanceof SPIPluginLifecycle )
                {
                    SPIPluginLifecycle lifeCycleSpi = (SPIPluginLifecycle) lifecycle;
                    injectables.addAll( lifeCycleSpi.start( neoServer ) );
                }
                else
                {
                    injectables.addAll( lifecycle.start( graphDatabaseService, new ConfigWrappingConfiguration( configuration ) ) );
                }
            }
        }
        return injectables;
    }

    private boolean hasPackage( PluginLifecycle pluginLifecycle, Iterable<String> packageNames )
    {
        String lifecyclePackageName = pluginLifecycle.getClass().getPackage().getName();
        for ( String packageName : packageNames )
        {
            if ( lifecyclePackageName.startsWith( packageName ) )
            {
                return true;
            }
        }
        return false;
    }

    public void stop()
    {
        for ( PluginLifecycle pluginLifecycle : lifecycles )
        {
            pluginLifecycle.stop();
        }
    }
}
