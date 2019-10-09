/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.logging.internal.LogService;
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
    private final IndexProviderMap indexProviderMap;
    private final PageCache pageCache;
    private final LogTailScanner tailScanner;
    private final JobScheduler jobScheduler;
    private final DatabaseLayout databaseLayout;
    private final LegacyTransactionLogsLocator legacyLogsLocator;
    private final StorageEngineFactory storageEngineFactory;

    public DatabaseMigrator( FileSystemAbstraction fs, Config config, LogService logService, IndexProviderMap indexProviderMap, PageCache pageCache,
            LogTailScanner tailScanner, JobScheduler jobScheduler, DatabaseLayout databaseLayout, LegacyTransactionLogsLocator legacyLogsLocator,
            StorageEngineFactory storageEngineFactory )
    {
        this.fs = fs;
        this.config = config;
        this.logService = logService;
        this.indexProviderMap = indexProviderMap;
        this.pageCache = pageCache;
        this.tailScanner = tailScanner;
        this.jobScheduler = jobScheduler;
        this.databaseLayout = databaseLayout;
        this.legacyLogsLocator = legacyLogsLocator;
        this.storageEngineFactory = storageEngineFactory;
    }

    /**
     * Performs construction of {@link StoreUpgrader} and all of the necessary participants and performs store
     * migration if that is required.
     */
    public void migrate() throws IOException
    {
        StoreVersionCheck storeVersionCheck = storageEngineFactory.versionCheck( fs, databaseLayout, config, pageCache, logService );
        StoreUpgrader storeUpgrader = new StoreUpgrader( storeVersionCheck,
                new VisibleMigrationProgressMonitor( logService.getUserLog( DatabaseMigrator.class ) ), config, fs, logService.getInternalLogProvider(),
                tailScanner, legacyLogsLocator );

        // Get all the participants from the storage engine and add them where they want to be
        var storeParticipants = storageEngineFactory.migrationParticipants( fs, config, pageCache, jobScheduler, logService );
        storeParticipants.forEach( storeUpgrader::addParticipant );

        IndexConfigMigrator indexConfigMigrator = new IndexConfigMigrator( fs, config, pageCache, logService, storageEngineFactory, indexProviderMap,
                logService.getUserLog( IndexConfigMigrator.class ) );
        storeUpgrader.addParticipant( indexConfigMigrator );

        IndexProviderMigrator indexProviderMigrator = new IndexProviderMigrator( fs, config, pageCache, logService, storageEngineFactory );
        storeUpgrader.addParticipant( indexProviderMigrator );

        // Do individual index provider migration last because they may delete files that we need in earlier steps.
        this.indexProviderMap.accept( provider -> storeUpgrader.addParticipant( provider.storeMigrationParticipant( fs, pageCache, storageEngineFactory ) ) );

        storeUpgrader.migrateIfNeeded( databaseLayout );
    }
}
