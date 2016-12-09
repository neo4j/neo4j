/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.participant.LegacyIndexMigrator;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.kernel.spi.legacyindex.IndexImplementation;
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
    private final SchemaIndexProvider schemaIndexProvider;
    private final LabelScanStoreProvider labelScanStoreProvider;
    private final Map<String,IndexImplementation> indexProviders;
    private final PageCache pageCache;
    private final RecordFormats format;

    public DatabaseMigrator(
            MigrationProgressMonitor progressMonitor, FileSystemAbstraction fs,
            Config config, LogService logService, SchemaIndexProvider schemaIndexProvider,
            LabelScanStoreProvider labelScanStoreProvider,
            Map<String,IndexImplementation> indexProviders, PageCache pageCache,
            RecordFormats format )
    {
        this.progressMonitor = progressMonitor;
        this.fs = fs;
        this.config = config;
        this.logService = logService;
        this.schemaIndexProvider = schemaIndexProvider;
        this.labelScanStoreProvider = labelScanStoreProvider;
        this.indexProviders = indexProviders;
        this.pageCache = pageCache;
        this.format = format;
    }

    /**
     * Performs construction of {@link StoreUpgrader} and all of the necessary participants and performs store
     * migration if that is required.
     * @param storeDir store to migrate
     */
    public void migrate(File storeDir)
    {
        LogProvider logProvider = logService.getInternalLogProvider();
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( fs, new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fs ),
                        format );
        StoreUpgrader storeUpgrader = new StoreUpgrader( upgradableDatabase, progressMonitor, config, fs,
                logProvider );

        StoreMigrationParticipant schemaMigrator = schemaIndexProvider.storeMigrationParticipant( fs, pageCache,
                labelScanStoreProvider );
        LegacyIndexMigrator legacyIndexMigrator = new LegacyIndexMigrator( indexProviders, logProvider );
        StoreMigrator storeMigrator = new StoreMigrator( fs, pageCache, config, logService, schemaIndexProvider );

        storeUpgrader.addParticipant( schemaMigrator );
        storeUpgrader.addParticipant( legacyIndexMigrator );
        storeUpgrader.addParticipant( storeMigrator );
        storeUpgrader.migrateIfNeeded( storeDir );
    }
}
