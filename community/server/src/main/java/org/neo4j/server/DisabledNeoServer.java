/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.net.URI;
import java.util.Collections;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.server.database.LifecycleManagingDatabase;
import org.neo4j.server.plugins.DisabledPluginManager;
import org.neo4j.server.plugins.PluginManager;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.rest.transactional.DisabledTransactionRegistry;
import org.neo4j.server.rest.transactional.TransactionRegistry;

import static org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;
import static org.neo4j.server.AbstractNeoServer.NEO4J_IS_STARTING_MESSAGE;
import static org.neo4j.server.exception.ServerStartupErrors.translateToServerStartupError;

public class DisabledNeoServer implements NeoServer
{
    private final Database db;
    private final Config config;

    private final LifeSupport life = new LifeSupport();

    public DisabledNeoServer( GraphFactory graphFactory, Dependencies dependencies, Config config )
    {
        this.db = new LifecycleManagingDatabase( config, graphFactory, dependencies );
        this.config = config;

        life.add( db );
        dependencies.userLogProvider().getLog( getClass() ).info( NEO4J_IS_STARTING_MESSAGE );
    }

    @Override
    public void start()
    {
        try
        {
            life.start();
        }
        catch ( Throwable t )
        {
            life.shutdown();
            throw translateToServerStartupError( t );
        }
    }

    @Override
    public void stop()
    {
        life.stop();
    }

    @Override
    public Config getConfig()
    {
        return config;
    }

    @Override
    public Database getDatabase()
    {
        return db;
    }

    @Override
    public TransactionRegistry getTransactionRegistry()
    {
        return DisabledTransactionRegistry.INSTANCE;
    }

    @Override
    public PluginManager getExtensionManager()
    {
        return DisabledPluginManager.INSTANCE;
    }

    @Override
    public URI baseUri()
    {
        throw new UnsupportedOperationException( "Neo4j server is disabled" );
    }

    @Override
    public Iterable<AdvertisableService> getServices()
    {
        return Collections.emptyList();
    }
}
