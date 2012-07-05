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
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.cache.CacheProvider;

/**
 * An implementation of {@link GraphDatabaseService} that is used to embed Neo4j
 * in an application. You typically instantiate it by invoking the
 * {@link #EmbeddedGraphDatabase(String) single argument constructor} that takes
 * a path to a directory where Neo4j will store its data files, as such:
 *
 * <pre>
 * <code>
 * GraphDatabaseService graphDb = new EmbeddedGraphDatabase( &quot;var/graphdb&quot; );
 * // ... use Neo4j
 * graphDb.shutdown();
 * </code>
 * </pre>
 *
 * For more information, see {@link GraphDatabaseService}.
 */
public class EmbeddedGraphDatabase extends InternalAbstractGraphDatabase
{
    /**
     * Creates an embedded {@link GraphDatabaseService} with a store located in
     * <code>storeDir</code>, which will be created if it doesn't already exist.
     *
     * @param storeDir the store directory for the Neo4j store files
     */
    public EmbeddedGraphDatabase( String storeDir )
    {
        this( storeDir, new HashMap<String, String>() );
    }

    /**
     * A non-standard way of creating an embedded {@link GraphDatabaseService}
     * with a set of configuration parameters.
     * <p>
     * Creates an embedded {@link GraphDatabaseService} with a store located in
     * <code>storeDir</code>, which will be created if it doesn't already exist.
     *
     * @param storeDir the store directory for the db files
     * @param params configuration parameters
     */
    public EmbeddedGraphDatabase( String storeDir, Map<String,String> params )
    {
        this( storeDir, params, Service.load( IndexProvider.class ), Service.load( KernelExtension.class ),
                Service.load( CacheProvider.class ) );
    }
    
    public EmbeddedGraphDatabase( String storeDir, Map<String,String> params, Iterable<IndexProvider> indexProviders,
            Iterable<KernelExtension> kernelExtensions, Iterable<CacheProvider> cacheProviders )
    {
        super( storeDir, params, indexProviders, kernelExtensions, cacheProviders );
        
        run();
    }
}