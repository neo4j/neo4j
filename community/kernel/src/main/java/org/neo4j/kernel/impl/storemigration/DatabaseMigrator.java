/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
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
    private final JobScheduler jobScheduler;
    private final DatabaseLayout databaseLayout;
    private final LegacyTransactionLogsLocator legacyLogsLocator;
    private final StorageEngineFactory storageEngineFactory;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;
    private final DatabaseHealth databaseHealth;

    public DatabaseMigrator(
            FileSystemAbstraction fs, Config config, LogService logService, DependencyResolver dependencyResolver, PageCache pageCache,
            JobScheduler jobScheduler, DatabaseLayout databaseLayout, StorageEngineFactory storageEngineFactory,
            PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker, DatabaseHealth databaseHealth )
    {
        this.fs = fs;
        this.config = config;
        this.logService = logService;
        this.dependencyResolver = dependencyResolver;
        this.pageCache = pageCache;
        this.jobScheduler = jobScheduler;
        this.databaseLayout = databaseLayout;
        this.legacyLogsLocator = new LegacyTransactionLogsLocator( config, databaseLayout );
        this.storageEngineFactory = storageEngineFactory;
        this.pageCacheTracer = pageCacheTracer;
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
        StoreVersionCheck versionCheck = storageEngineFactory.versionCheck( fs, databaseLayout, config, pageCache, logService, pageCacheTracer );
        LogsUpgrader logsUpgrader = new LogsUpgrader(
                fs, storageEngineFactory, databaseLayout, pageCache, legacyLogsLocator, config, dependencyResolver, pageCacheTracer, memoryTracker,
                databaseHealth, forceUpgrade );
        Log userLog = logService.getUserLog( DatabaseMigrator.class );
        VisibleMigrationProgressMonitor progress = new VisibleMigrationProgressMonitor( userLog );
        LogProvider logProvider = logService.getInternalLogProvider();
        StoreUpgrader storeUpgrader = new StoreUpgrader( versionCheck, progress, config, fs, logProvider, logsUpgrader, pageCacheTracer );

        // Get all the participants from the storage engine and add them where they want to be
        var storeParticipants = storageEngineFactory.migrationParticipants(
                fs, config, pageCache, jobScheduler, logService, pageCacheTracer, memoryTracker );
        storeParticipants.forEach( storeUpgrader::addParticipant );

        IndexProviderMap indexProviderMap = dependencyResolver.resolveDependency( IndexProviderMap.class );
        IndexConfigMigrator indexConfigMigrator = new IndexConfigMigrator( fs, config, pageCache, logService, storageEngineFactory, indexProviderMap,
                logService.getUserLog( IndexConfigMigrator.class ), pageCacheTracer, memoryTracker );
        storeUpgrader.addParticipant( indexConfigMigrator );

        IndexProviderMigrator indexProviderMigrator = new IndexProviderMigrator(
                fs, config, pageCache, logService, storageEngineFactory, pageCacheTracer, memoryTracker );
        storeUpgrader.addParticipant( indexProviderMigrator );

        // Do individual index provider migration last because they may delete files that we need in earlier steps.
        indexProviderMap.accept( provider -> storeUpgrader.addParticipant( provider.storeMigrationParticipant( fs, pageCache, storageEngineFactory ) ) );

        try
        {
            storeUpgrader.migrateIfNeeded( databaseLayout, forceUpgrade );
        }
        catch ( Exception e )
        {
            userLog.error( "Error upgrading database. Database left intact and will likely not be able to start: " + e.toString() );
            throw e;
        }
    }
}
