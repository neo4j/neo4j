/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.state;

import java.util.ArrayList;
import java.util.Collection;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.index.schema.AbstractIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.FulltextV1IndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.FulltextV2IndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.PointIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.RangeIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.TextV1IndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.TextV2IndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.TextV3IndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.TokenIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.VectorIndexProviderFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;

public class StaticIndexProviderMapFactory {

    public static StaticIndexProviderMap create(
            LifeSupport life,
            Config databaseConfig,
            KernelVersionProvider kernelVersionProvider,
            PageCache pageCache,
            FileSystemAbstraction fs,
            LogService logService,
            Monitors monitors,
            DatabaseReadOnlyChecker readOnlyChecker,
            HostedOnMode mode,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            DatabaseLayout databaseLayout,
            TokenHolders tokenHolders,
            JobScheduler scheduler,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer) {
        return create(
                life,
                databaseConfig,
                kernelVersionProvider,
                pageCache,
                fs,
                logService,
                monitors,
                readOnlyChecker,
                mode,
                recoveryCleanupWorkCollector,
                databaseLayout,
                tokenHolders,
                scheduler,
                contextFactory,
                pageCacheTracer,
                new Dependencies());
    }

    public static StaticIndexProviderMap create(
            LifeSupport life,
            Config databaseConfig,
            KernelVersionProvider kernelVersionProvider,
            PageCache pageCache,
            FileSystemAbstraction fs,
            LogService logService,
            Monitors monitors,
            DatabaseReadOnlyChecker readOnlyChecker,
            HostedOnMode mode,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            DatabaseLayout databaseLayout,
            TokenHolders tokenHolders,
            JobScheduler scheduler,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            DependencyResolver dependencies) {

        Collection<AbstractIndexProviderFactory<?>> indexProviderFactories = new ArrayList<>();
        indexProviderFactories.add(new TokenIndexProviderFactory());
        indexProviderFactories.add(new RangeIndexProviderFactory());
        indexProviderFactories.add(new PointIndexProviderFactory());
        indexProviderFactories.add(new TextV1IndexProviderFactory());
        indexProviderFactories.add(new TextV2IndexProviderFactory());
        indexProviderFactories.add(new TextV3IndexProviderFactory());
        indexProviderFactories.add(new FulltextV1IndexProviderFactory());
        indexProviderFactories.add(new FulltextV2IndexProviderFactory());
        for (VectorIndexVersion vectorIndexVersion : VectorIndexVersion.KNOWN_VERSIONS) {
            indexProviderFactories.add(new VectorIndexProviderFactory(vectorIndexVersion));
        }

        return new StaticIndexProviderMap(
                dependencies,
                createIndexProviders(
                        pageCache,
                        fs,
                        logService,
                        monitors,
                        databaseConfig,
                        kernelVersionProvider,
                        readOnlyChecker,
                        mode,
                        recoveryCleanupWorkCollector,
                        databaseLayout,
                        tokenHolders,
                        scheduler,
                        contextFactory,
                        pageCacheTracer,
                        dependencies,
                        life,
                        indexProviderFactories));
    }

    private static Iterable<IndexProvider> createIndexProviders(
            PageCache pageCache,
            FileSystemAbstraction fs,
            LogService logService,
            Monitors monitors,
            Config config,
            KernelVersionProvider kernelVersionProvider,
            DatabaseReadOnlyChecker readOnlyChecker,
            HostedOnMode mode,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            DatabaseLayout databaseLayout,
            TokenHolders tokenHolders,
            JobScheduler scheduler,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            DependencyResolver dependencyResolver,
            LifeSupport life,
            Collection<AbstractIndexProviderFactory<?>> indexProviderFactories) {
        Collection<IndexProvider> indexProviders = new ArrayList<>(indexProviderFactories.size());
        for (AbstractIndexProviderFactory<?> indexProviderFactory : indexProviderFactories) {
            indexProviders.add(life.add(indexProviderFactory.create(
                    pageCache,
                    fs,
                    logService,
                    monitors,
                    config,
                    kernelVersionProvider,
                    readOnlyChecker,
                    mode,
                    recoveryCleanupWorkCollector,
                    databaseLayout,
                    tokenHolders,
                    scheduler,
                    contextFactory,
                    pageCacheTracer,
                    dependencyResolver)));
        }
        return indexProviders;
    }
}
