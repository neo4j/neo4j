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

import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.logging.LogProvider;
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

    public DatabaseMigrator( FileSystemAbstraction fs, Config config, LogService logService, IndexProviderMap indexProviderMap, PageCache pageCache,
            LogTailScanner tailScanner, JobScheduler jobScheduler, DatabaseLayout databaseLayout, LegacyTransactionLogsLocator legacyLogsLocator )
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
    }

    /**
     * Performs construction of {@link StoreUpgrader} and all of the necessary participants and performs store
     * migration if that is required.
     */
    public void migrate() throws IOException
    {
        LogProvider logProvider = logService.getInternalLogProvider();

        StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine( Service.load( StorageEngineFactory.class ) );
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( fs, pageCache, databaseLayout, config, jobScheduler, logService );
        StoreVersionCheck storeVersionCheck = storageEngineFactory.versionCheck( dependencies );

        StoreUpgrader storeUpgrader = new StoreUpgrader( storeVersionCheck,
                new VisibleMigrationProgressMonitor( logService.getUserLog( DatabaseMigrator.class ) ), config, fs, pageCache, logProvider,
                tailScanner, legacyLogsLocator );

        this.indexProviderMap.accept(
            provider -> storeUpgrader.addParticipant( provider.storeMigrationParticipant( fs, pageCache ) ) );
        storeUpgrader.addParticipant( storageEngineFactory.migrationParticipant( dependencies ) );
        storeUpgrader.addParticipant( new NativeLabelScanStoreMigrator( fs, pageCache, config, storageEngineFactory ) );
        storeUpgrader.migrateIfNeeded( databaseLayout );
    }
}
