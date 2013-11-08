/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.logging.Logger;

import static org.neo4j.kernel.logging.LogbackWeakDependency.logbackOrDefaultToClassic;

/**
 * Wraps a neo4j database in lifecycle management. This is intermediate, and will go away once we have an internal
 * database that exposes lifecycle cleanly.
 */
public class LifecycleManagingDatabase implements Database
{

    public static final GraphFactory EMBEDDED = new GraphFactory()
    {
        @Override
        public GraphDatabaseAPI newGraphDatabase( Config config, Function<Config, Logging> loggingProvider,
                                                Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                                Iterable<CacheProvider> cacheProviders,
                                                Iterable<TransactionInterceptorProvider> txInterceptorProviders )
        {
            return new EmbeddedGraphDatabase( config, loggingProvider, kernelExtensions, cacheProviders,
                    txInterceptorProviders );
        }
    };

    public interface GraphFactory
    {
        GraphDatabaseAPI newGraphDatabase( Config config, Function<Config, Logging> loggingProvider,
                                           Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                           Iterable<CacheProvider> cacheProviders,
                                           Iterable<TransactionInterceptorProvider> txInterceptorProviders );
    }

    public static Database.Factory lifecycleManagingDatabase( final GraphFactory graphDbFactory )
    {
        return new Factory()
        {
            @Override
            public Database newDatabase( Config config, Iterable<KernelExtensionFactory<?>> kernelExtensions )
            {
                return new LifecycleManagingDatabase( config, graphDbFactory, kernelExtensions );
            }
        };
    }

    public static final Logger log = Logger.getLogger(Database.class);

    private final Config dbConfig;
    private final GraphFactory dbFactory;

    private final GraphDatabaseFactoryState factoryState = new GraphDatabaseFactoryState();

    private boolean isRunning = false;
    private GraphDatabaseAPI graph;

    public LifecycleManagingDatabase( Config dbConfig, GraphFactory dbFactory,
                                      Iterable<KernelExtensionFactory<?>> extensions )
    {
        this.dbConfig = dbConfig;
        this.dbFactory = dbFactory;
        this.factoryState.addKernelExtensions( extensions );
    }

    @Override
    public String getLocation()
    {
        return dbConfig.get(GraphDatabaseSettings.store_dir).getAbsolutePath();
    }

    @Override
    public org.neo4j.graphdb.index.Index<Relationship> getRelationshipIndex( String name )
    {
        RelationshipIndex index = graph.index().forRelationships( name );
        if ( index == null )
        {
            throw new RuntimeException( "No index for [" + name + "]" );
        }
        return index;
    }

    @Override
    public org.neo4j.graphdb.index.Index<Node> getNodeIndex( String name )
    {
        org.neo4j.graphdb.index.Index<Node> index = graph.index()
                .forNodes( name );
        if ( index == null )
        {
            throw new RuntimeException( "No index for [" + name + "]" );
        }
        return index;
    }

    @Override
    public IndexManager getIndexManager()
    {
        return graph.index();
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
            this.graph = dbFactory.newGraphDatabase( dbConfig,
                    logbackOrDefaultToClassic(),
                    factoryState.getKernelExtension(),
                    factoryState.getCacheProviders(),
                    factoryState.getTransactionInterceptorProviders() );
            isRunning = true;
            log.info( "Successfully started database" );
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
            if ( this.graph != null )
            {
                this.graph.shutdown();
                isRunning = false;
                this.graph = null;
                log.info( "Successfully stopped database" );
            }
        }
        catch ( Exception e )
        {
            log.error( String.format("Database did not stop cleanly. Reason [%s]", e.getMessage()) );
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
