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
package org.neo4j.kernel;

import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.logging.Logging;

public class GraphDatabaseDependencies implements InternalAbstractGraphDatabase.Dependencies
{
    private final Logging logging;
    private final Iterable<Class<?>> settingsClasses;
    private final Iterable<IndexProvider> indexProviders;
    private final Iterable<KernelExtensionFactory<?>> kernelExtensions;
    private final Iterable<CacheProvider> cacheProviders;
    private final Iterable<TransactionInterceptorProvider> transactionInterceptorProviders;

    @SuppressWarnings( "deprecation" )
    public GraphDatabaseDependencies(
            Logging logging,
            Iterable<Class<?>> settingsClasses,
            Iterable<IndexProvider> indexProviders,
            Iterable<KernelExtensionFactory<?>> kernelExtensions, Iterable<CacheProvider> cacheProviders,
            Iterable<TransactionInterceptorProvider> transactionInterceptorProviders )
    {
        this.logging = logging;
        this.settingsClasses = settingsClasses;
        this.indexProviders = indexProviders;
        this.kernelExtensions = kernelExtensions;
        this.cacheProviders = cacheProviders;
        this.transactionInterceptorProviders = transactionInterceptorProviders;
    }

    @Override
    public Logging logging()
    {
        return logging;
    }

    @Override
    public Iterable<Class<?>> settingsClasses()
    {
        return settingsClasses;
    }

    @Override
    public Iterable<IndexProvider> indexProviders()
    {
        return indexProviders;
    }

    @Override
    public Iterable<KernelExtensionFactory<?>> kernelExtensions()
    {
        return kernelExtensions;
    }

    @Override
    public Iterable<CacheProvider> cacheProviders()
    {
        return cacheProviders;
    }

    @Override
    public Iterable<TransactionInterceptorProvider> transactionInterceptorProviders()
    {
        return transactionInterceptorProviders;
    }
}
