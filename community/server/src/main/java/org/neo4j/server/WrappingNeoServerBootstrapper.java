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

import java.io.File;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.configuration.ConfigurationBuilder.ConfiguratorWrappingConfigurationBuilder;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;

/**
 * A bootstrapper for the Neo4j Server that takes an already instantiated
 * {@link org.neo4j.kernel.GraphDatabaseAPI}, and optional configuration, and launches a
 * server using that database.
 *
 * @deprecated This class is for internal use only and will be moved to an internal package in a future release.
 * Please use Neo4j Server and plugins or un-managed extensions for bespoke solutions.
 */
@Deprecated
public class WrappingNeoServerBootstrapper extends Bootstrapper
{
    private final GraphDatabaseAPI db;
    private final ConfigurationBuilder configurator;

    /**
     * Create an instance with default settings.
     *
     * @param db
     */
    public WrappingNeoServerBootstrapper( GraphDatabaseAPI db )
    {
        this( db, new ServerConfigurator( db ) );
    }

    public void start()
    {
        super.start( null );
    }

    /**
     * Should use the new constructor with {@link ConfigurationBuilder}
     */
    @Deprecated
    public WrappingNeoServerBootstrapper( GraphDatabaseAPI db, Configurator configurator )
    {
        this( db, new ConfiguratorWrappingConfigurationBuilder( configurator ) );
    }

    private WrappingNeoServerBootstrapper( GraphDatabaseAPI db, ConfigurationBuilder configurator )
    {
        this.db = db;
        this.configurator = configurator;
    }

    @Override
    protected NeoServer createNeoServer( Config config, GraphDatabaseDependencies dependencies,
            LogProvider userLogProvider )
    {
        return new WrappingNeoServer(db, configurator );
    }

    @Override
    protected Config createConfig( Log log, File file, Pair<String, String>[] configOverrides )
    {
        return WrappingNeoServer.toConfig( configurator );
    }
}
