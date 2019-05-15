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
package org.neo4j.server.database;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Log;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * Wraps a neo4j database in lifecycle management. This is intermediate, and will go away once we have an internal
 * database that exposes lifecycle cleanly.
 */
public class LifecycleManagingDatabaseService implements DatabaseService
{
    private final Config config;
    private final GraphFactory dbFactory;
    private final ExternalDependencies dependencies;
    private final Log log;

    private boolean isRunning;
    private DatabaseManagementService managementService;

    public LifecycleManagingDatabaseService( Config config, GraphFactory dbFactory,
            ExternalDependencies dependencies )
    {
        this.config = config;
        this.dbFactory = dbFactory;
        this.dependencies = dependencies;
        this.log = dependencies.userLogProvider().getLog( getClass() );
    }

    @Override
    public DatabaseManagementService getDatabaseManagementService()
    {
        return managementService;
    }

    @Override
    public GraphDatabaseFacade getDatabase()
    {
        return getDatabase( config.get( GraphDatabaseSettings.default_database ) );
    }

    @Override
    public GraphDatabaseFacade getSystemDatabase()
    {
        return getDatabase( SYSTEM_DATABASE_NAME );
    }

    @Override
    public GraphDatabaseFacade getDatabase( String databaseName ) throws DatabaseNotFoundException
    {
        return (GraphDatabaseFacade) managementService.database( databaseName );
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
        log.info( "Starting..." );
        managementService = dbFactory.newDatabaseManagementService( config, dependencies );
        isRunning = true;
        log.info( "Started." );
    }

    @Override
    public void stop()
    {
        if ( managementService != null )
        {
            log.info( "Stopping..." );
            managementService.shutdown();
            isRunning = false;
            managementService = null;
            log.info( "Stopped." );
        }
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public boolean isRunning()
    {
        return isRunning;
    }
}
