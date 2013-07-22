/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.graphdb.factory;

import static org.neo4j.graphdb.factory.GraphDatabaseSetting.TRUE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.read_only;

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.index.IndexIterable;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;

/**
 * Creates a {@link org.neo4j.graphdb.GraphDatabaseService}.
 * 
 * Use {@link #newEmbeddedDatabase(String)} or
 * {@link #newEmbeddedDatabaseBuilder(String)} to create a database instance.
 */
public class GraphDatabaseFactory
{
    private final GraphDatabaseFactoryState state;

    public GraphDatabaseFactory()
    {
        this( new GraphDatabaseFactoryState() );
    }

    protected GraphDatabaseFactory(GraphDatabaseFactoryState state)
    {
        this.state = state;
    }

    protected GraphDatabaseFactoryState getCurrentState()
    {
        return state;
    }

    protected GraphDatabaseFactoryState getStateCopy()
    {
        return new GraphDatabaseFactoryState( getCurrentState() );
    }

    public GraphDatabaseService newEmbeddedDatabase( String path )
    {
        return newEmbeddedDatabaseBuilder( path ).newGraphDatabase();
    }

    public GraphDatabaseBuilder newEmbeddedDatabaseBuilder( final String path )
    {
        final GraphDatabaseFactoryState state = getStateCopy();
        return new GraphDatabaseBuilder( new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            public GraphDatabaseService newDatabase( Map<String, String> config )
            {
                config.put( "ephemeral", "false" );

                if ( TRUE.equalsIgnoreCase( config.get( read_only.name() ) ) )
                {
                    return new EmbeddedReadOnlyGraphDatabase( path, config,
                            state.getIndexProviders(),
                            state.getKernelExtension(),
                            state.getCacheProviders(),
                            state.getTransactionInterceptorProviders() );
                }
                else
                {
                    return new EmbeddedGraphDatabase( path, config,
                            state.getIndexProviders(),
                            state.getKernelExtension(),
                            state.getCacheProviders(),
                            state.getTransactionInterceptorProviders() );
                }
            }
        } );
    }

    public Iterable<IndexProvider> getIndexProviders()
    {
        return getCurrentState().getIndexProviders();
    }

    public GraphDatabaseFactory setIndexProviders( IndexIterable indexIterable )
    {
        getCurrentState().setIndexProviders( indexIterable );
        return this;
    }

    public Iterable<KernelExtensionFactory<?>> getKernelExtension()
    {
        return getCurrentState().getKernelExtension();
    }

    public GraphDatabaseFactory addKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        getCurrentState().addKernelExtensions( newKernelExtensions );
        return this;
    }

    public GraphDatabaseFactory setKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        getCurrentState().setKernelExtensions( newKernelExtensions );
        return this;
    }

    public List<CacheProvider> getCacheProviders()
    {
        return getCurrentState().getCacheProviders();
    }

    public GraphDatabaseFactory setCacheProviders( Iterable<CacheProvider> newCacheProviders )
    {
        getCurrentState().setCacheProviders( newCacheProviders );
        return this;
    }

    public List<TransactionInterceptorProvider> getTransactionInterceptorProviders()
    {
        return getCurrentState().getTransactionInterceptorProviders();
    }

    public GraphDatabaseFactory setTransactionInterceptorProviders( Iterable<TransactionInterceptorProvider> transactionInterceptorProviders )
    {
        getCurrentState().setTransactionInterceptorProviders( transactionInterceptorProviders );
        return this;
    }

}
