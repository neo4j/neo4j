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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.index.IndexIterable;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;

public class GraphDatabaseFactoryState
{
    private List<IndexProvider> indexProviders;
    private List<KernelExtensionFactory<?>> kernelExtensions;
    private List<CacheProvider> cacheProviders;
    private List<TransactionInterceptorProvider> txInterceptorProviders;

    public GraphDatabaseFactoryState() {
        indexProviders = Iterables.toList( Service.load( IndexProvider.class ) );
        kernelExtensions = new ArrayList<KernelExtensionFactory<?>>();
        for ( KernelExtensionFactory factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
        cacheProviders = Iterables.toList( Service.load( CacheProvider.class ) );
        txInterceptorProviders = Iterables.toList( Service.load( TransactionInterceptorProvider.class ) );
    }

    public GraphDatabaseFactoryState( GraphDatabaseFactoryState previous )
    {
        indexProviders = new ArrayList<IndexProvider>( previous.indexProviders );
        kernelExtensions = new ArrayList<KernelExtensionFactory<?>>( previous.kernelExtensions );
        cacheProviders = new ArrayList<CacheProvider>( previous.cacheProviders );
        txInterceptorProviders = new ArrayList<TransactionInterceptorProvider>( previous.txInterceptorProviders );
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
            kernelExtensions.add( newKernelExtension );
        }
    }

    public List<CacheProvider> getCacheProviders()
    {
        return cacheProviders;
    }

    public void setCacheProviders( Iterable<CacheProvider> newCacheProviders )
    {
        cacheProviders.clear();
        for ( CacheProvider newCacheProvider : newCacheProviders )
        {
            cacheProviders.add( newCacheProvider );
        }
    }

    public List<TransactionInterceptorProvider> getTransactionInterceptorProviders()
    {
        return txInterceptorProviders;
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
