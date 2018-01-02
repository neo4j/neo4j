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
package org.neo4j.server.database;

import java.io.File;

import org.neo4j.graphdb.Result;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.logging.Log;
import org.neo4j.server.web.ServerInternalSettings;

/**
 * Wraps a neo4j database in lifecycle management. This is intermediate, and will go away once we have an internal
 * database that exposes lifecycle cleanly.
 */
public class LifecycleManagingDatabase implements Database
{
    static final String CYPHER_WARMUP_QUERY =
            "MATCH (a:` Arbitrary label name that really doesn't matter `) RETURN a LIMIT 0";

    public interface GraphFactory
    {
        GraphDatabaseAPI newGraphDatabase( Config config, GraphDatabaseFacadeFactory.Dependencies dependencies );
    }

    public static Database.Factory lifecycleManagingDatabase( final GraphFactory graphDbFactory )
    {
        return new Factory()
        {
            @Override
            public Database newDatabase( Config config, GraphDatabaseFacadeFactory.Dependencies dependencies )
            {
                return new LifecycleManagingDatabase( config, graphDbFactory, dependencies );
            }
        };
    }

    private final Config config;
    private final GraphFactory dbFactory;
    private final GraphDatabaseFacadeFactory.Dependencies dependencies;
    private final Log log;

    private boolean isRunning = false;
    private GraphDatabaseAPI graph;

    public LifecycleManagingDatabase( Config config, GraphFactory dbFactory,
            GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        this.config = config;
        this.dbFactory = dbFactory;
        this.dependencies = dependencies;
        this.log = dependencies.userLogProvider().getLog( getClass() );
    }

    @Override
    public String getLocation()
    {
        File file = config.get( ServerInternalSettings.legacy_db_location );
        return file.getAbsolutePath();
    }

    @Override
    public GraphDatabaseAPI getGraph()
    {
        return graph;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        this.graph = dbFactory.newGraphDatabase( config, dependencies );
        // in order to speed up testing, they should not run the preload, but in production it pays to do it.
        if ( !isInTestMode() )
        {
            preLoadCypherCompiler();
        }

        isRunning = true;
        log.info( "Successfully started database" );
    }

    @Override
    public void stop() throws Throwable
    {
        if ( graph != null )
        {
            graph.shutdown();
            isRunning = false;
            graph = null;
            log.info( "Successfully stopped database" );
        }
    }

    @Override
    public void shutdown() throws Throwable
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
