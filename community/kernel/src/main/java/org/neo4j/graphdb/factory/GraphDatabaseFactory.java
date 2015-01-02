/**
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
package org.neo4j.graphdb.factory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.index.IndexIterable;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.DefaultGraphDatabaseDependencies;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.InternalAbstractGraphDatabase.Dependencies;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.graphdb.factory.GraphDatabaseSetting.TRUE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.read_only;

/**
 * Creates a {@link org.neo4j.graphdb.GraphDatabaseService}.
 *
 * Use {@link #newEmbeddedDatabase(String)} or
 * {@link #newEmbeddedDatabaseBuilder(String)} to create a database instance.
 */
public class GraphDatabaseFactory
{
    protected FileSystemAbstraction fileSystem;
    protected Logging logging;
    protected Iterable<Class<?>> settingsClasses;
    protected List<IndexProvider> indexProviders;
    protected List<KernelExtensionFactory<?>> kernelExtensions;
    protected List<CacheProvider> cacheProviders;
    protected List<TransactionInterceptorProvider> txInterceptorProviders;

    public GraphDatabaseFactory()
    {
        Dependencies defaultDependencies = new DefaultGraphDatabaseDependencies();

        logging = defaultDependencies.logging(); // probably null == not provided externally
        settingsClasses = defaultDependencies.settingsClasses();
        indexProviders = Iterables.toList( defaultDependencies.indexProviders() );
        kernelExtensions = new ArrayList<KernelExtensionFactory<?>>();
        for ( KernelExtensionFactory factory : defaultDependencies.kernelExtensions() )
        {
            kernelExtensions.add( factory );
        }
        cacheProviders = Iterables.toList( defaultDependencies.cacheProviders() );
        txInterceptorProviders = Iterables.toList( defaultDependencies.transactionInterceptorProviders() );
    }

    public GraphDatabaseService newEmbeddedDatabase( String path )
    {
        return newEmbeddedDatabaseBuilder( path ).newGraphDatabase();
    }

    public GraphDatabaseBuilder newEmbeddedDatabaseBuilder( final String path )
    {
        return new GraphDatabaseBuilder( new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            public GraphDatabaseService newDatabase( Map<String, String> config )
            {
                config.put( "ephemeral", "false" );
                Dependencies dependencies = databaseDependencies();
                if ( TRUE.equalsIgnoreCase( config.get( read_only.name() ) ) )
                {
                    return new EmbeddedReadOnlyGraphDatabase( path, config, dependencies );
                }
                else
                {
                    return new EmbeddedGraphDatabase( path, config, dependencies );
                }
            }
        } );
    }

    protected GraphDatabaseDependencies databaseDependencies()
    {
        return new GraphDatabaseDependencies(
                logging, settingsClasses, indexProviders, kernelExtensions, cacheProviders,
                txInterceptorProviders );
    }

    public Iterable<IndexProvider> getIndexProviders()
    {
        return indexProviders;
    }

    /**
     * Sets an {@link org.neo4j.graphdb.index.IndexProvider} iterable source.
     * {@link org.neo4j.kernel.ListIndexIterable} is a flexible provider that works well with
     * dependency injection.
     *
     * @param indexIterable It's actually Iterable<IndexProvider>, but internally typecasted
     *                      to workaround bug https://issues.apache.org/jira/browse/ARIES-834 .
     */
    public void setIndexProviders( IndexIterable indexIterable )
    {
        indexProviders.clear();
        for ( IndexProvider indexProvider : indexIterable )
        {
            this.indexProviders.add( indexProvider );
        }
    }

    public Iterable<KernelExtensionFactory<?>> getKernelExtension()
    {
        return kernelExtensions;
    }

    public void setKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        kernelExtensions.clear();
        addKernelExtensions( newKernelExtensions );
    }

    public void addKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        for ( KernelExtensionFactory<?> newKernelExtension : newKernelExtensions )
        {
            addKernelExtension( newKernelExtension );
        }
    }

    public void addKernelExtension( KernelExtensionFactory<?> newKernelExtension )
    {
        kernelExtensions.add( newKernelExtension );
    }

    public void setCacheProviders( Iterable<CacheProvider> newCacheProviders )
    {
        cacheProviders.clear();
        for ( CacheProvider newCacheProvider : newCacheProviders )
        {
            cacheProviders.add( newCacheProvider );
        }
    }

    public void setTransactionInterceptorProviders( Iterable<TransactionInterceptorProvider>
                                                            transactionInterceptorProviders )
    {
        txInterceptorProviders.clear();
        for ( TransactionInterceptorProvider newTxInterceptorProvider : transactionInterceptorProviders )
        {
            txInterceptorProviders.add( newTxInterceptorProvider );
        }
    }
}
