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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

import java.nio.file.Path;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.logging.InternalLog;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;
import org.neo4j.util.VisibleForTesting;

public class PointIndexProviderFactory extends AbstractIndexProviderFactory<PointIndexProvider> {
    @Override
    protected Class<?> loggingClass() {
        return PointIndexProvider.class;
    }

    @Override
    public IndexProviderDescriptor descriptor() {
        return AllIndexProviderDescriptors.POINT_DESCRIPTOR;
    }

    @Override
    protected PointIndexProvider internalCreate(
            PageCache pageCache,
            FileSystemAbstraction fs,
            Monitors monitors,
            String monitorTag,
            Config config,
            DatabaseReadOnlyChecker readOnlyChecker,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            DatabaseLayout databaseLayout,
            InternalLog log,
            TokenHolders tokenHolders,
            JobScheduler scheduler,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            DependencyResolver dependencyResolver) {
        return create(
                pageCache,
                databaseLayout.databaseDirectory(),
                fs,
                monitors,
                monitorTag,
                config,
                readOnlyChecker,
                recoveryCleanupWorkCollector,
                contextFactory,
                pageCacheTracer,
                databaseLayout.getDatabaseName(),
                dependencyResolver);
    }

    @VisibleForTesting
    public static PointIndexProvider create(
            PageCache pageCache,
            Path storeDir,
            FileSystemAbstraction fs,
            Monitors monitors,
            String monitorTag,
            Config config,
            DatabaseReadOnlyChecker readOnlyChecker,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            String databaseName,
            DependencyResolver dependencyResolver) {
        IndexDirectoryStructure.Factory directoryStructure = directoriesByProvider(storeDir);
        DatabaseIndexContext databaseIndexContext = DatabaseIndexContext.builder(
                        pageCache, fs, contextFactory, pageCacheTracer, databaseName)
                .withMonitors(monitors)
                .withTag(monitorTag)
                .withReadOnlyChecker(readOnlyChecker)
                .withDependencyResolver(dependencyResolver)
                .build();
        return new PointIndexProvider(databaseIndexContext, directoryStructure, recoveryCleanupWorkCollector, config);
    }
}
