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
package org.neo4j.kernel;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;

/**
 * An implementation of {@link GraphDatabaseService} that is used to embed Neo4j
 * in an application. You typically instantiate it by using
 * {@link GraphDatabaseFactory} like so:
 * <p/>
 * 
 * <pre>
 * <code>
 * GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( &quot;var/graphdb&quot; );
 * // ... use Neo4j
 * graphDb.shutdown();
 * </code>
 * </pre>
 * <p/>
 * For more information, see {@link GraphDatabaseService}.
 */
public class EmbeddedGraphDatabase extends InternalAbstractGraphDatabase
{
    /**
     * Creates an embedded {@link GraphDatabaseService} with a store located in
     * <code>storeDir</code>, which will be created if it doesn't already exist.
     * 
     * This is deprecated. Use {@link GraphDatabaseFactory} instead.
     * 
     * @param storeDir the store directory for the Neo4j store files
     */
    @Deprecated
    public EmbeddedGraphDatabase( String storeDir )
    {
        this( storeDir, new HashMap<String, String>() );
    }

    /**
     * A non-standard way of creating an embedded {@link GraphDatabaseService}
     * with a set of configuration parameters.
     * <p/>
     * Creates an embedded {@link GraphDatabaseService} with a store located in
     * <code>storeDir</code>, which will be created if it doesn't already exist.
     * 
     * This is deprecated. Use {@link GraphDatabaseFactory} instead.
     * 
     * @param storeDir the store directory for the db files
     * @param params configuration parameters
     */
    @Deprecated
    public EmbeddedGraphDatabase( String storeDir, Map<String, String> params )
    {
        this( storeDir, params,
                Service.load( IndexProvider.class ),
                Iterables.<KernelExtensionFactory<?>,KernelExtensionFactory>cast( Service.load( KernelExtensionFactory.class ) ),
                Service.load( CacheProvider.class ),
                Service.load( TransactionInterceptorProvider.class ) );
    }

    /**
     * Internal constructor used by {@link org.neo4j.graphdb.factory.GraphDatabaseFactory}
     *
     * @param storeDir
     * @param params
     * @param indexProviders
     * @param kernelExtensions
     * @param cacheProviders
     * @param txInterceptorProviders
     */
    public EmbeddedGraphDatabase( String storeDir, Map<String, String> params, Iterable<IndexProvider> indexProviders,
                                  Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                  Iterable<CacheProvider> cacheProviders,
                                  Iterable<TransactionInterceptorProvider> txInterceptorProviders )
    {
        super( storeDir, params, Iterables.<Class<?>, Class<?>>iterable( (Class<?>) GraphDatabaseSettings.class ),
                indexProviders, kernelExtensions, cacheProviders, txInterceptorProviders );

        run();
    }

    @Override
    protected boolean isHighlyAvailable()
    {
        return false;
    }
}
