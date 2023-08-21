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

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.LoggingMonitor;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;

public abstract class AbstractIndexProviderFactory<T extends IndexProvider> {

    public T create(
            PageCache pageCache,
            FileSystemAbstraction fs,
            LogService logService,
            Monitors monitors,
            Config config,
            DatabaseReadOnlyChecker readOnlyChecker,
            HostedOnMode mode,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            DatabaseLayout databaseLayout,
            TokenHolders tokenHolders,
            JobScheduler scheduler,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            DependencyResolver dependencyResolver) {
        if (HostedOnMode.SINGLE != mode) {
            // if running as part of cluster indexes should be writable to allow catchup process to accept transactions
            readOnlyChecker = DatabaseReadOnlyChecker.writable();
        }
        InternalLog log = logService.getInternalLogProvider().getLog(loggingClass());
        String monitorTag = descriptor().toString();
        monitors.addMonitorListener(new LoggingMonitor(log), monitorTag);
        return internalCreate(
                pageCache,
                fs,
                monitors,
                monitorTag,
                config,
                readOnlyChecker,
                recoveryCleanupWorkCollector,
                databaseLayout,
                log,
                tokenHolders,
                scheduler,
                contextFactory,
                pageCacheTracer,
                dependencyResolver);
    }

    protected abstract Class<?> loggingClass();

    public abstract IndexProviderDescriptor descriptor();

    protected abstract T internalCreate(
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
            DependencyResolver dependencyResolver);
}
