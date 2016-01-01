/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
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
            public Database newDatabase( Config config, Logging logging )
            {
                return new LifecycleManagingDatabase( config, graphDbFactory, logging );
            }
        };
    }

    private final Config dbConfig;
    private final GraphFactory dbFactory;
    private final GraphDatabaseFactoryState factoryState = new GraphDatabaseFactoryState();
    private final ConsoleLogger log;

    private boolean isRunning = false;
    private GraphDatabaseAPI graph;
    private ExecutionEngine executionEngine;
    private Logging logging;

    public LifecycleManagingDatabase( Config dbConfig, GraphFactory dbFactory, Logging logging )
    {
        this.dbConfig = dbConfig;
        this.dbFactory = dbFactory;
        this.logging = logging;
        this.factoryState.setLogging( logging );
        this.log = logging.getConsoleLog( getClass() );
    }

    @Override
    public Logging getLogging()
    {
        return logging;
    }

    @Override
    public String getLocation()
    {
        return dbConfig.get(GraphDatabaseSettings.store_dir).getAbsolutePath();
    }

    @Override
    public GraphDatabaseAPI getGraph()
    {
        return graph;
    }

    @Override
    public ExecutionEngine executionEngine()
    {
        return executionEngine;
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
            this.graph = dbFactory.newGraphDatabase( getLocation(), dbConfig.getParams(),
                    factoryState.databaseDependencies() );
            this.executionEngine = new ExecutionEngine( graph );
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
