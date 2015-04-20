/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.InternalAbstractGraphDatabase.Dependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;

/**
 * Wraps a neo4j database in lifecycle management. This is intermediate, and will go away once we have an internal
 * database that exposes lifecycle cleanly.
 */
public class LifecycleManagingDatabase implements Database
{
    public static final GraphFactory EMBEDDED = new GraphFactory()
    {
        @Override
        public GraphDatabaseAPI newGraphDatabase( String storeDir, Map<String,String> params, Dependencies dependencies )
        {
            return new EmbeddedGraphDatabase( storeDir, params, dependencies );
        }
    };

    public interface GraphFactory
    {
        GraphDatabaseAPI newGraphDatabase( String storeDir, Map<String,String> params, Dependencies dependencies );
    }

    public static Database.Factory lifecycleManagingDatabase( final GraphFactory graphDbFactory )
    {
        return new Factory()
        {
            @Override
            public Database newDatabase(Config config, Dependencies dependencies)
            {
                return new LifecycleManagingDatabase( config, graphDbFactory, dependencies );
            }
        };
    }

    private final Config dbConfig;
    private final GraphFactory dbFactory;
    private final Dependencies dependencies;
    private final ConsoleLogger log;

    private boolean isRunning = false;
    private GraphDatabaseAPI graph;

    public LifecycleManagingDatabase(Config dbConfig, GraphFactory dbFactory, Dependencies dependencies)
    {
        this.dbConfig = dbConfig;
        this.dbFactory = dbFactory;
        this.dependencies = dependencies;
        this.log = dependencies.logging().getConsoleLog( getClass() );
    }

    @Override
    public Logging getLogging()
    {
        return dependencies.logging();
    }

    @Override
    public String getLocation()
    {
        File file = dbConfig.get( GraphDatabaseSettings.store_dir );
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
        try
        {
            this.graph = dbFactory.newGraphDatabase( getLocation(), dbConfig.getParams(), dependencies );
            isRunning = true;
            log.log( "Successfully started database" );
        }
        catch ( Exception e )
        {
            log.error( "Failed to start database.", e );
            throw e;
        }
    }

    @Override
    public void stop() throws Throwable
    {
        try
        {
            if ( graph != null )
            {
                graph.shutdown();
                isRunning = false;
                graph = null;
                log.log( "Successfully stopped database" );
            }
        }
        catch ( Exception e )
        {
            log.error( "Database did not stop cleanly. Reason [%s]", e.getMessage() );
            throw e;
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
}
