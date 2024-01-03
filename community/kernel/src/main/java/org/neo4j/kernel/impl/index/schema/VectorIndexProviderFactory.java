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

import static org.neo4j.kernel.api.impl.index.storage.DirectoryFactory.directoryFactory;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexProvider;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.logging.InternalLog;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;

public class VectorIndexProviderFactory extends AbstractIndexProviderFactory<VectorIndexProvider> {
    private final VectorIndexVersion version;

    public VectorIndexProviderFactory(VectorIndexVersion version) {
        this.version = version;
    }

    @Override
    protected Class<?> loggingClass() {
        return VectorIndexProvider.class;
    }

    @Override
    public IndexProviderDescriptor descriptor() {
        return version.descriptor();
    }

    @Override
    protected VectorIndexProvider internalCreate(
            PageCache pageCache,
            FileSystemAbstraction fs,
            Monitors monitors,
            String monitorTag,
            Config config,
            DatabaseReadOnlyChecker readOnlyDatabaseChecker,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            DatabaseLayout databaseLayout,
            InternalLog log,
            TokenHolders tokenHolders,
            JobScheduler scheduler,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            DependencyResolver dependencyResolver) {
        return new VectorIndexProvider(
                version,
                fs,
                directoryFactory(fs),
                directoriesByProvider(databaseLayout.databaseDirectory()),
                monitors,
                config,
                readOnlyDatabaseChecker,
                scheduler);
    }
}
