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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersionCheck;

/**
 * DatabaseMigrator collects all dependencies required for store migration,
 * prepare and construct all store upgrade participants in correct order and allow clients just migrate store
 * specified by provided location.
 *
 * @see StoreUpgrader
 */
public class DatabaseMigrator
{
    private final FileSystemAbstraction fs;
    private final Config config;
    private final LogService logService;
    private final DependencyResolver dependencyResolver;
    private final PageCache pageCache;
    private final PageCacheTracer pageCacheTracer;
    private final JobScheduler jobScheduler;
    private final DatabaseLayout databaseLayout;
    private final StorageEngineFactory storageEngineFactory;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;
    private final DatabaseHealth databaseHealth;

    public DatabaseMigrator(
            FileSystemAbstraction fs, Config config, LogService logService, DependencyResolver dependencyResolver, PageCache pageCache,
            PageCacheTracer pageCacheTracer, JobScheduler jobScheduler, DatabaseLayout databaseLayout, StorageEngineFactory storageEngineFactory,
            CursorContextFactory contextFactory, MemoryTracker memoryTracker, DatabaseHealth databaseHealth )
    {
        this.fs = fs;
        this.config = config;
        this.logService = logService;
        this.dependencyResolver = dependencyResolver;
        this.pageCache = pageCache;
        this.pageCacheTracer = pageCacheTracer;
        this.jobScheduler = jobScheduler;
        this.databaseLayout = databaseLayout;
        this.contextFactory = contextFactory;
        this.storageEngineFactory = storageEngineFactory;
        this.memoryTracker = memoryTracker;
        this.databaseHealth = databaseHealth;
    }

    /**
     * Performs construction of {@link StoreUpgrader} and all of the necessary participants and performs store
     * migration if that is required.
     *
     * @param forceUpgrade Ignore the value of the {@link GraphDatabaseSettings#allow_upgrade} setting.
     */
    public void migrate( boolean forceUpgrade ) throws IOException
    {
        StoreVersionCheck versionCheck = storageEngineFactory.versionCheck( fs, databaseLayout, config, pageCache, logService, contextFactory );
        var logsUpgrader = new LogsUpgrader( fs, storageEngineFactory, databaseLayout, pageCache, config, dependencyResolver,
                                                      memoryTracker, databaseHealth, contextFactory );
        InternalLog userLog = logService.getUserLog( DatabaseMigrator.class );
        VisibleMigrationProgressMonitor progress = new VisibleMigrationProgressMonitor( userLog );
        InternalLogProvider logProvider = logService.getInternalLogProvider();
        StoreUpgrader storeUpgrader = new StoreUpgrader( storageEngineFactory, versionCheck, progress, config, fs, logProvider, logsUpgrader, contextFactory );

        // Get all the participants from the storage engine and add them where they want to be
        var storeParticipants = storageEngineFactory.migrationParticipants(
                fs, config, pageCache, jobScheduler, logService, memoryTracker, pageCacheTracer, contextFactory );
        storeParticipants.forEach( storeUpgrader::addParticipant );

        IndexProviderMap indexProviderMap = dependencyResolver.resolveDependency( IndexProviderMap.class );

        // Do individual index provider migration last because they may delete files that we need in earlier steps.
        indexProviderMap.accept(
                provider -> storeUpgrader.addParticipant( provider.storeMigrationParticipant( fs, pageCache, storageEngineFactory, contextFactory ) ) );

        try
        {
            storeUpgrader.migrateIfNeeded( databaseLayout, forceUpgrade );
        }
        catch ( Exception e )
        {
            userLog.error( "Error upgrading database. Database left intact and will likely not be able to start: " + e );
            throw e;
        }
    }
}
