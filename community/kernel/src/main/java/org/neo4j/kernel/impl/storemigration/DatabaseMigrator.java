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

import java.io.File;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.participant.CountsMigrator;
import org.neo4j.kernel.impl.storemigration.participant.ExplicitIndexMigrator;
import org.neo4j.kernel.impl.storemigration.participant.NativeLabelScanStoreMigrator;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;
import org.neo4j.logging.LogProvider;

/**
 * DatabaseMigrator collects all dependencies required for store migration,
 * prepare and construct all store upgrade participants in correct order and allow clients just migrate store
 * specified by provided location.
 *
 * @see StoreUpgrader
 */
public class DatabaseMigrator
{
    private final MigrationProgressMonitor progressMonitor;
    private final FileSystemAbstraction fs;
    private final Config config;
    private final LogService logService;
    private final IndexProviderMap indexProviderMap;
    private final Map<String,IndexImplementation> indexProviders;
    private final PageCache pageCache;
    private final RecordFormats format;
    private final LogTailScanner tailScanner;

    public DatabaseMigrator(
            MigrationProgressMonitor progressMonitor, FileSystemAbstraction fs,
            Config config, LogService logService, IndexProviderMap indexProviderMap,
            Map<String,IndexImplementation> indexProviders, PageCache pageCache,
            RecordFormats format, LogTailScanner tailScanner )
    {
        this.progressMonitor = progressMonitor;
        this.fs = fs;
        this.config = config;
        this.logService = logService;
        this.indexProviderMap = indexProviderMap;
        this.indexProviders = indexProviders;
        this.pageCache = pageCache;
        this.format = format;
        this.tailScanner = tailScanner;
    }

    /**
     * Performs construction of {@link StoreUpgrader} and all of the necessary participants and performs store
     * migration if that is required.
     * @param storeDir store to migrate
     */
    public void migrate( File storeDir )
    {
        LogProvider logProvider = logService.getInternalLogProvider();
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( new StoreVersionCheck( pageCache ), format, tailScanner );
        StoreUpgrader storeUpgrader = new StoreUpgrader( upgradableDatabase, progressMonitor, config, fs, pageCache,
                logProvider );

        ExplicitIndexMigrator explicitIndexMigrator = new ExplicitIndexMigrator( fs, indexProviders, logProvider );
        StoreMigrator storeMigrator = new StoreMigrator( fs, pageCache, config, logService );
        NativeLabelScanStoreMigrator nativeLabelScanStoreMigrator =
                new NativeLabelScanStoreMigrator( fs, pageCache, config );
        CountsMigrator countsMigrator = new CountsMigrator( fs, pageCache, config );

        indexProviderMap.accept(
                provider -> storeUpgrader.addParticipant( provider.storeMigrationParticipant( fs, pageCache ) ) );
        storeUpgrader.addParticipant( explicitIndexMigrator );
        storeUpgrader.addParticipant( storeMigrator );
        storeUpgrader.addParticipant( nativeLabelScanStoreMigrator );
        storeUpgrader.addParticipant( countsMigrator );
        storeUpgrader.migrateIfNeeded( storeDir );
    }
}
