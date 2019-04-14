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
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Log;

/**
 * Wraps a neo4j database in lifecycle management. This is intermediate, and will go away once we have an internal
 * database that exposes lifecycle cleanly.
 */
public class LifecycleManagingDatabase implements Database
{
    static final String CYPHER_WARMUP_QUERY =
            "MATCH (a:` This query is just used to load the cypher compiler during warmup. Please ignore `) RETURN a LIMIT 0";

    private final Config config;
    private final GraphFactory dbFactory;
    private final ExternalDependencies dependencies;
    private final Log log;

    private boolean isRunning;
    private DatabaseManagementService managementService;
    private GraphDatabaseFacade graph;

    public LifecycleManagingDatabase( Config config, GraphFactory dbFactory,
            ExternalDependencies dependencies )
    {
        this.config = config;
        this.dbFactory = dbFactory;
        this.dependencies = dependencies;
        this.log = dependencies.userLogProvider().getLog( getClass() );
    }

    @Override
    public GraphDatabaseFacade getGraph()
    {
        return graph;
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
        this.graph = (GraphDatabaseFacade) managementService.database( config.get( GraphDatabaseSettings.default_database ) );
        // in order to speed up testing, they should not run the preload, but in production it pays to do it.
        if ( !isInTestMode() )
        {
            preLoadCypherCompiler();
        }

        isRunning = true;
        log.info( "Started." );
    }

    @Override
    public void stop()
    {
        if ( graph != null )
        {
            log.info( "Stopping..." );
            managementService.shutdown();
            isRunning = false;
            graph = null;
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

    private void preLoadCypherCompiler()
    {
        // Execute a single Cypher query to pre-load the compiler to make the first user-query snappy
        try
        {
            //noinspection EmptyTryBlock
            try ( Result ignore = this.graph.execute( CYPHER_WARMUP_QUERY ) )
            {
                // empty by design
            }
        }
        catch ( Exception ignore )
        {
            // This is only an attempt at warming up the database.
            // It's not a critical failure.
        }
    }

    protected boolean isInTestMode()
    {
        // The assumption here is that assertions are only enabled during testing.
        boolean testing = false;
        assert testing = true : "yes, this should be an assignment!";
        return testing;
    }
}
