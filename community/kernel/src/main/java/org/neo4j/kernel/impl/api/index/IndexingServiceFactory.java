/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingControllerFactory;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.kernel.impl.transaction.state.storeview.IndexStoreViewFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;

/**
 * Factory to create {@link IndexingService}
 */
public final class IndexingServiceFactory
{
    private IndexingServiceFactory()
    {
    }

    public static IndexingService createIndexingService( Config config,
            JobScheduler scheduler,
            IndexProviderMap providerMap,
            IndexStoreViewFactory indexStoreViewFactory,
            TokenNameLookup tokenNameLookup,
            Iterable<IndexDescriptor> indexRules,
            LogProvider internalLogProvider,
            IndexMonitor monitor,
            SchemaState schemaState,
            IndexStatisticsStore indexStatisticsStore,
            PageCacheTracer pageCacheTracer,
            MemoryTracker memoryTracker,
            String databaseName,
            DatabaseReadOnlyChecker readOnlyChecker )
    {
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( config );
        IndexMapReference indexMapRef = new IndexMapReference();
        IndexSamplingControllerFactory factory = new IndexSamplingControllerFactory( samplingConfig, indexStatisticsStore, scheduler,
                tokenNameLookup, internalLogProvider, pageCacheTracer, config, databaseName );
        IndexSamplingController indexSamplingController = factory.create( indexMapRef );
        IndexProxyCreator proxySetup =
                new IndexProxyCreator( samplingConfig, indexStatisticsStore, providerMap, tokenNameLookup, internalLogProvider );

        return new IndexingService( proxySetup, providerMap, indexMapRef, indexStoreViewFactory, indexRules,
                indexSamplingController, tokenNameLookup, scheduler, schemaState,
                internalLogProvider, monitor, indexStatisticsStore, pageCacheTracer, memoryTracker, databaseName, readOnlyChecker, config );
    }
}
