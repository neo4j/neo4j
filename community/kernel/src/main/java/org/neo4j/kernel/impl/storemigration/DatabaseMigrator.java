/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.store.format.RecordFormatPropertyConfigurator;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

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

    public DatabaseMigrator(
            FileSystemAbstraction fs,
            Config config, LogService logService, IndexProviderMap indexProviderMap,
            PageCache pageCache,
            LogTailScanner tailScanner, JobScheduler jobScheduler )
    {
        this.fs = fs;
        this.config = config;
        this.logService = logService;
        this.indexProviderMap = indexProviderMap;
        this.pageCache = pageCache;
        this.tailScanner = tailScanner;
        this.jobScheduler = jobScheduler;
    }

    /**
     * Performs construction of {@link StoreUpgrader} and all of the necessary participants and performs store
     * migration if that is required.
     * @param databaseLayout database to migrate
     */
    public void migrate( DatabaseLayout databaseLayout )
    {
        LogProvider logProvider = logService.getInternalLogProvider();
        final RecordFormats format = selectStoreFormats( config, databaseLayout, fs, pageCache, logService );
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( pageCache ), format, tailScanner );
        StoreUpgrader storeUpgrader = new StoreUpgrader( upgradableDatabase,
            new VisibleMigrationProgressMonitor( logService.getUserLog( DatabaseMigrator.class ) ), config, fs, pageCache, logProvider );

        StoreMigrator storeMigrator = new StoreMigrator( fs, pageCache, config, logService, jobScheduler );
        NativeLabelScanStoreMigrator nativeLabelScanStoreMigrator = new NativeLabelScanStoreMigrator( fs, pageCache, config );
        CountsMigrator countsMigrator = new CountsMigrator( fs, pageCache, config );

        indexProviderMap.accept(
            provider -> storeUpgrader.addParticipant( provider.storeMigrationParticipant( fs, pageCache ) ) );
        storeUpgrader.addParticipant( storeMigrator );
        storeUpgrader.addParticipant( nativeLabelScanStoreMigrator );
        storeUpgrader.addParticipant( countsMigrator );
        storeUpgrader.migrateIfNeeded( databaseLayout );
    }

    private static RecordFormats selectStoreFormats( Config config, DatabaseLayout databaseLayout, FileSystemAbstraction fs, PageCache pageCache,
        LogService logService )
    {
        LogProvider logging = logService.getInternalLogProvider();
        RecordFormats formats = RecordFormatSelector.selectNewestFormat( config, databaseLayout, fs, pageCache, logging );
        new RecordFormatPropertyConfigurator( formats, config ).configure();
        return formats;
    }
}
