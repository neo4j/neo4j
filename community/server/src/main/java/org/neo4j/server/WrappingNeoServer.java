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
package org.neo4j.server;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.configuration.ConfigurationBuilder.ConfiguratorWrappingConfigurationBuilder;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.web.ServerInternalSettings;

import static java.util.Arrays.asList;
import static org.neo4j.server.database.WrappedDatabase.wrappedDatabase;

/**
 * @deprecated This class is for internal use only and will be moved to an internal package in a future release.
 * Please use Neo4j Server and plugins or un-managed extensions for bespoke solutions.
 */
@Deprecated
public class WrappingNeoServer extends CommunityNeoServer
{
    public WrappingNeoServer( GraphDatabaseAPI db )
    {
        this( db, new ServerConfigurator( db ) );
    }

    /*
     Note: All this interface is public, but deprecated. Because of that, it exposes a vertiable mess of config
     options. This will all go away in 3.0, along with all other config that isn't the Neo4j Config API.
     */

    @Deprecated
    public WrappingNeoServer( GraphDatabaseAPI db, Configurator configurator )
    {
        this( db, new ConfiguratorWrappingConfigurationBuilder(configurator ) );
    }

    @Deprecated
    public WrappingNeoServer( GraphDatabaseAPI db, ConfigurationBuilder configurator )
    {
        this( db, configurator, db.getDependencyResolver().resolveDependency( LogService.class ).getUserLogProvider() );
    }

    private WrappingNeoServer( GraphDatabaseAPI db, ConfigurationBuilder configurator, LogProvider logProvider )
    {
        super( toConfig( configurator ),
                wrappedDatabase( db ),
                GraphDatabaseDependencies.newDependencies().userLogProvider(
                        logProvider
                ).monitors( db.getDependencyResolver().resolveDependency( Monitors.class ) ), logProvider );
    }

    static Config toConfig( ConfigurationBuilder configurator )
    {
        Config config = new Config( configurator.configuration().getParams() );
        config.augment( configurator.getDatabaseTuningProperties() );
        config.registerSettingsClasses( asList(ServerSettings.class,
                ServerInternalSettings.class,
                GraphDatabaseSettings.class ) );
        return config;
    }
}
