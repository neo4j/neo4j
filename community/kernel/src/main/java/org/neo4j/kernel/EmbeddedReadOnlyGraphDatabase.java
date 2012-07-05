/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.cache.CacheProvider;

/**
 * A read-only version of {@link EmbeddedGraphDatabase}.
 */
public final class EmbeddedReadOnlyGraphDatabase extends InternalAbstractGraphDatabase
{
    private static Map<String, String> readOnlyParams = new HashMap<String, String>();

    static
    {
        readOnlyParams.put( GraphDatabaseSettings.read_only.name(), GraphDatabaseSetting.TRUE );
    }

    /**
     * Creates an embedded {@link GraphDatabaseService} with a store located in
     * <code>storeDir</code>. If the directory shouldn't exist or isn't a neo4j
     * store an exception will be thrown.
     *
     * @param storeDir the store directory for the Neo4j store files
     */
    public EmbeddedReadOnlyGraphDatabase( String storeDir )
    {
        this( storeDir, readOnlyParams );
    }

    /**
     * A non-standard way of creating an embedded {@link GraphDatabaseService}
     * with a set of configuration parameters. Will most likely be removed in
     * future releases.
     * <p>
     * Creates an embedded {@link GraphDatabaseService} with a store located in
     * <code>storeDir</code>. If the directory shouldn't exist or isn't a neo4j
     * store an exception will be thrown.
     *
     * @param storeDir the store directory for the db files
     * @param params configuration parameters
     */
    public EmbeddedReadOnlyGraphDatabase( String storeDir,
            Map<String, String> params )
    {
        this( storeDir, params, Service.load( IndexProvider.class ), Service.load( KernelExtension.class ),
                Service.load( CacheProvider.class ));
    }

    public EmbeddedReadOnlyGraphDatabase( String storeDir,
            Map<String, String> params, Iterable<IndexProvider> indeProviders, Iterable<KernelExtension> kernelExtensions,
            Iterable<CacheProvider> cacheProviders )
    {
        super( storeDir, addReadOnly(params), indeProviders, kernelExtensions, cacheProviders );
        run();
    }

    private static Map<String, String> addReadOnly( Map<String, String> params )
    {
        params.putAll( readOnlyParams );
        return params;
    }

    public KernelEventHandler registerKernelEventHandler(
            KernelEventHandler handler )
    {
        throw new UnsupportedOperationException();
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        throw new UnsupportedOperationException();
    }

    public KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler handler )
    {
        throw new UnsupportedOperationException();
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        throw new UnsupportedOperationException();
    }
}
