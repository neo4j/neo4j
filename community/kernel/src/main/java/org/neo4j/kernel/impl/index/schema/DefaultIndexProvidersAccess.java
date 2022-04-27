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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.impl.factory.DbmsInfo.TOOL;
import static org.neo4j.kernel.impl.index.schema.SchemaIndexExtensionLoader.instantiateExtensions;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMapFactory;
import org.neo4j.kernel.lifecycle.LifeContainer;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;

public class DefaultIndexProvidersAccess extends LifeContainer implements IndexProvidersAccess {
    private final StorageEngineFactory storageEngineFactory;
    private final FileSystemAbstraction fileSystem;
    private final Config databaseConfig;
    private final JobScheduler jobScheduler;
    private final LogService logService;
    private final PageCacheTracer pageCacheTracer;
    private final CursorContextFactory contextFactory;

    public DefaultIndexProvidersAccess(
            StorageEngineFactory storageEngineFactory,
            FileSystemAbstraction fileSystem,
            Config databaseConfig,
            JobScheduler jobScheduler,
            LogService logService,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory) {

        this.storageEngineFactory = storageEngineFactory;
        this.fileSystem = fileSystem;
        this.databaseConfig = databaseConfig;
        this.jobScheduler = jobScheduler;
        this.logService = logService;
        this.pageCacheTracer = pageCacheTracer;
        this.contextFactory = contextFactory;
    }

    @Override
    public IndexProviderMap access(
            PageCache pageCache, DatabaseLayout layout, DatabaseReadOnlyChecker readOnlyChecker) {
        var tokenHolders = storageEngineFactory.loadReadOnlyTokens(
                fileSystem, layout, databaseConfig, pageCache, false, contextFactory);
        var monitors = new Monitors();
        var extensions = life.add(instantiateExtensions(
                layout,
                fileSystem,
                databaseConfig,
                logService,
                pageCache,
                jobScheduler,
                immediate(),
                TOOL, // We use TOOL context because it's true, and also because it uses the 'single' operational mode,
                // which is important.
                monitors,
                tokenHolders,
                pageCacheTracer,
                readOnly()));
        return life.add(StaticIndexProviderMapFactory.create(
                life,
                databaseConfig,
                pageCache,
                fileSystem,
                logService,
                monitors,
                readOnlyChecker,
                TOOL,
                immediate(),
                layout,
                tokenHolders,
                jobScheduler,
                contextFactory,
                extensions));
    }
}
