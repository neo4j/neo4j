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
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_VERSION;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP;

@RunWith( Parameterized.class )
public class StoreMigratorIT
{
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( directory ).around( fileSystemRule ).around( pageCacheRule );

    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();
    private final FileSystemAbstraction fs = fileSystemRule.get();

    @Parameterized.Parameter( 0 )
    public String version;

    @Parameterized.Parameter( 1 )
    public LogPosition expectedLogPosition;

    @Parameterized.Parameter( 2 )
    public Function<TransactionId, Boolean> txIdComparator;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> versions()
    {
        return Arrays.asList(
                new Object[]{
                        StandardV2_0.STORE_VERSION,
                        new LogPosition( BASE_TX_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET ),
                        txInfoAcceptanceOnIdAndTimestamp( 1039, UNKNOWN_TX_COMMIT_TIMESTAMP )
                },
                new Object[]{
                        StandardV2_1.STORE_VERSION,
                        new LogPosition( BASE_TX_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET ),
                        txInfoAcceptanceOnIdAndTimestamp( 38, UNKNOWN_TX_COMMIT_TIMESTAMP)
                },
                new Object[]{
                        StandardV2_2.STORE_VERSION,
                        new LogPosition( 2, BASE_TX_LOG_BYTE_OFFSET ),
                        txInfoAcceptanceOnIdAndTimestamp( 38, UNKNOWN_TX_COMMIT_TIMESTAMP )
                },
                new Object[]{
                        StandardV2_3.STORE_VERSION, new LogPosition( 3, 169 ),
                        txInfoAcceptanceOnIdAndTimestamp( 39, UNKNOWN_TX_COMMIT_TIMESTAMP )
                }
        );
    }

    private static Function<TransactionId,Boolean> txInfoAcceptanceOnIdAndTimestamp( long id, long timestamp )
    {
        return txInfo -> txInfo.transactionId() == id &&
               txInfo.commitTimestamp() == timestamp;
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
        migrator.migrate( storeDirectory, migrationDir, progressMonitor.startSection( "section" ),
                versionToMigrateFrom, upgradableDatabase.currentVersion() );

        // WHEN simulating resuming the migration
        progressMonitor = new SilentMigrationProgressMonitor();
        migrator = new StoreMigrator( fs, pageCache, Config.empty(), logService, schemaIndexProvider );
        migrator.moveMigratedFiles( migrationDir, storeDirectory, versionToMigrateFrom, upgradableDatabase.currentVersion() );

        // THEN starting the new store should be successful
        StoreFactory storeFactory =
                new StoreFactory( storeDirectory, pageCache, fs, logService.getInternalLogProvider() );
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
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( fs, new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fs ),
                         selectFormat() );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory ).storeVersion();
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, Config.empty(), logService, schemaIndexProvider );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdirs( migrationDir );
        migrator.migrate( storeDirectory, migrationDir, progressMonitor.startSection( "section" ),
                versionToMigrateFrom, upgradableDatabase.currentVersion() );
        migrator.moveMigratedFiles( migrationDir, storeDirectory, versionToMigrateFrom,
                upgradableDatabase.currentVersion() );

        // WHEN simulating resuming the migration
        progressMonitor = new SilentMigrationProgressMonitor();
        migrator = new StoreMigrator( fs, pageCache, Config.empty(), logService, schemaIndexProvider );
        migrator.rebuildCounts( storeDirectory, versionToMigrateFrom, upgradableDatabase.currentVersion() );

        // THEN starting the new store should be successful
        StoreFactory storeFactory =
                new StoreFactory( storeDirectory, pageCache, fs, logService.getInternalLogProvider() );
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
        migrator.migrate( storeDirectory, migrationDir, progressMonitor.startSection( "section" ),
                versionToMigrateFrom, upgradableDatabase.currentVersion() );

        // THEN it should compute the correct last tx log position
        assertEquals( expectedLogPosition, migrator.readLastTxLogPosition( migrationDir ) );
    }

    @Test
    public void shouldComputeTheLastTxInfoCorrectly() throws Exception
    {
        // given
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
        fs.mkdir( migrationDir );

        // when
        migrator.migrate( storeDirectory, migrationDir, progressMonitor.startSection( "section" ),
                versionToMigrateFrom, upgradableDatabase.currentVersion() );

        // then
        assertTrue( txIdComparator.apply( migrator.readLastTxInformation( migrationDir ) ) );
    }

    private RecordFormats selectFormat()
    {
        return Standard.LATEST_RECORD_FORMATS;
    }
}
