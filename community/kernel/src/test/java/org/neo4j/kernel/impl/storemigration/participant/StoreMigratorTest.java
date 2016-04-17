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
package org.neo4j.kernel.impl.storemigration.participant;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.storemigration.participant.StoreMigrator.readLastTxLogPosition;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_VERSION;

@RunWith( Parameterized.class )
public class StoreMigratorTest
{
    @Rule
    public final TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    public final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();
    @Parameterized.Parameter( 0 )
    public String version;

    @Parameterized.Parameter( 1 )
    public LogPosition expectedLogPosition;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> versions()
    {
        return Arrays.asList(
                new Object[]{
                        StandardV2_0.STORE_VERSION, new LogPosition( BASE_TX_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET )},
                new Object[]{
                        StandardV2_1.STORE_VERSION, new LogPosition( BASE_TX_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET )},
                new Object[]{
                        StandardV2_2.STORE_VERSION, new LogPosition( 2, BASE_TX_LOG_BYTE_OFFSET )},
                new Object[]{
                        StandardV2_3.STORE_VERSION, new LogPosition( 3, 169 )}
        );
    }

    @Test
    public void shouldBeAbleToResumeMigrationOnMoving() throws Exception
    {
        // GIVEN a legacy database
        File storeDirectory = directory.graphDbDir();
        File prepare = directory.directory( "prepare" );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, storeDirectory, prepare );
        // and a state of the migration saying that it has done the actual migration
        LogService logService = NullLogService.getInstance();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( fs, new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fs ),
                        selectFormat() );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory ).storeVersion();
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, Config.empty(), logService, schemaIndexProvider );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdirs( migrationDir );
        migrator.migrate( storeDirectory, migrationDir, progressMonitor.startSection( "Test" ), versionToMigrateFrom,
                upgradableDatabase.currentVersion() );

        // WHEN simulating resuming the migration
        progressMonitor = new SilentMigrationProgressMonitor();
        migrator = new StoreMigrator( fs, pageCache, Config.empty(), logService, schemaIndexProvider);
        migrator.moveMigratedFiles( migrationDir, storeDirectory, versionToMigrateFrom,
                upgradableDatabase.currentVersion() );

        // THEN starting the new store should be successful
        StoreFactory storeFactory = new StoreFactory( fs, storeDirectory, pageCache, selectFormat(),
                logService.getInternalLogProvider() );
        storeFactory.openAllNeoStores().close();
    }

    @Test
    public void shouldBeAbleToResumeMigrationOnRebuildingCounts() throws Exception
    {
        // GIVEN a legacy database
        File storeDirectory = directory.graphDbDir();
        File prepare = directory.directory( "prepare" );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, storeDirectory, prepare );
        // and a state of the migration saying that it has done the actual migration
        LogService logService = NullLogService.getInstance();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fs, new StoreVersionCheck( pageCache ),
                new LegacyStoreVersionCheck( fs ), selectFormat() );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory ).storeVersion();
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, Config.empty(), logService, schemaIndexProvider );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdirs( migrationDir );
        migrator.migrate( storeDirectory, migrationDir, progressMonitor.startSection( "Test" ), versionToMigrateFrom,
                upgradableDatabase.currentVersion() );
        migrator.moveMigratedFiles( migrationDir, storeDirectory, versionToMigrateFrom,
                upgradableDatabase.currentVersion() );

        // WHEN simulating resuming the migration
        progressMonitor = new SilentMigrationProgressMonitor();
        migrator = new StoreMigrator( fs, pageCache, Config.empty(), logService, schemaIndexProvider );
        migrator.rebuildCounts( storeDirectory, versionToMigrateFrom, upgradableDatabase.currentVersion() );

        // THEN starting the new store should be successful
        StoreFactory storeFactory = new StoreFactory( fs, storeDirectory, pageCache, selectFormat(),
                logService.getInternalLogProvider() );
        storeFactory.openAllNeoStores().close();
    }

    @Test
    public void shouldComputeTheLastTxLogPositionCorrectly() throws Throwable
    {
        // GIVEN a legacy database
        File storeDirectory = directory.graphDbDir();
        File prepare = directory.directory( "prepare" );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, storeDirectory, prepare );
        // and a state of the migration saying that it has done the actual migration
        LogService logService = NullLogService.getInstance();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( fs, new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fs ),
                        selectFormat() );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory ).storeVersion();
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, Config.empty(), logService, schemaIndexProvider );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdirs( migrationDir );

        // WHEN migrating
        migrator.migrate( storeDirectory, migrationDir, progressMonitor.startSection( "Test" ),
                versionToMigrateFrom, upgradableDatabase.currentVersion() );

        // THEN it should compute the correct last tx log position
        assertEquals( expectedLogPosition, readLastTxLogPosition( fs, migrationDir ) );
    }

    private RecordFormats selectFormat()
    {
        return StandardV3_0.RECORD_FORMATS;
    }
}
