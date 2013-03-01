/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import java.util.Map;

import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.SchemaIndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;

/**
 * This is only for backwards compatibility with 1.8. Will be removed in the future. The right way to get this
 * is to instantiate through GraphDatabaseFactory.
 */
@Deprecated
public class HighlyAvailableGraphDatabase
    extends org.neo4j.kernel.ha.HighlyAvailableGraphDatabase
{
    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> config)
    {
        this(storeDir, config,
                Service.load( IndexProvider.class ),
                Iterables.<KernelExtensionFactory<?>, KernelExtensionFactory>cast(Service.load( KernelExtensionFactory.class )),
                Service.load( CacheProvider.class ),
                Service.load( TransactionInterceptorProvider.class ),
                Service.load( SchemaIndexProvider.class ) );
    }

    public HighlyAvailableGraphDatabase( String storeDir, Map<String, String> params, Iterable<IndexProvider> indexProviders,
            Iterable<KernelExtensionFactory<?>> kernelExtensions, Iterable<CacheProvider> cacheProviders,
            Iterable<TransactionInterceptorProvider> txInterceptorProviders,
            Iterable<SchemaIndexProvider> schemaIndexProviders )
    {
        super( storeDir, params, indexProviders, kernelExtensions, cacheProviders, txInterceptorProviders, schemaIndexProviders );
    }

    @Deprecated
    public void pullUpdates()
    {
        dependencyResolver.resolveDependency( UpdatePuller.class ).pullUpdates();
    }
}
