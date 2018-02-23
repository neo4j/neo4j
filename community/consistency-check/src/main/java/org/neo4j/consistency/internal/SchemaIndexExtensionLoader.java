/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.consistency.internal;

import java.io.File;

import org.neo4j.helpers.Service;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.extension.UnsatisfiedDependencyStrategies;
import org.neo4j.kernel.extension.dependency.AllByPrioritySelectionStrategy;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.spi.SimpleKernelContext;
import org.neo4j.kernel.impl.transaction.state.DefaultSchemaIndexProviderMap;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * Utility for loading {@link SchemaIndexProvider} instances from {@link KernelExtensions}.
 */
public class SchemaIndexExtensionLoader
{
    public static SchemaIndexProviderMap loadSchemaIndexProviders( KernelExtensions extensions )
    {
        AllByPrioritySelectionStrategy<SchemaIndexProvider> indexProviderSelection = new AllByPrioritySelectionStrategy<>();
        SchemaIndexProvider defaultIndexProvider =
                extensions.resolveDependency( SchemaIndexProvider.class, indexProviderSelection );
        return new DefaultSchemaIndexProviderMap( defaultIndexProvider,
                indexProviderSelection.lowerPrioritizedCandidates() );
    }

    @SuppressWarnings( "unchecked" )
    public static KernelExtensions instantiateKernelExtensions( File storeDir, FileSystemAbstraction fileSystem,
            Config config, LogService logService, PageCache pageCache,
            RecoveryCleanupWorkCollector recoveryCollector, DatabaseInfo databaseInfo, Monitors monitors )
    {
        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( fileSystem, config, logService, pageCache, recoveryCollector, monitors );
        @SuppressWarnings( "rawtypes" )
        Iterable kernelExtensions = Service.load( KernelExtensionFactory.class );
        KernelContext kernelContext = new SimpleKernelContext( storeDir, databaseInfo, deps );
        return new KernelExtensions( kernelContext, kernelExtensions, deps, UnsatisfiedDependencyStrategies.ignore() );
    }
}
