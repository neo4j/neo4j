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
package org.neo4j.kernel.impl.storemigration.participant;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP;

@RunWith( Parameterized.class )
public class StoreMigratorIT
{
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private static final Config CONFIG = Config.defaults( GraphDatabaseSettings.pagecache_memory, "8m" );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( directory ).around( fileSystemRule ).around( pageCacheRule );

    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private final LogService logService = new SimpleLogService( logProvider );
    private final Monitors monitors = new Monitors();
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
        return Arrays.<Object[]>asList(
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
        LogTailScanner tailScanner = getTailScanner( storeDirectory );
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache, tailScanner );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory ).storeVersion();
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, CONFIG, logService );
        CountsMigrator countsMigrator = new CountsMigrator( fs, pageCache, CONFIG );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdirs( migrationDir );
        migrator.migrate( storeDirectory, migrationDir, progressMonitor.startSection( "section" ), versionToMigrateFrom,
                upgradableDatabase.currentVersion() );
        countsMigrator
                .migrate( storeDirectory, migrationDir, progressMonitor.startSection( "section" ), versionToMigrateFrom,
                        upgradableDatabase.currentVersion() );

        // WHEN simulating resuming the migration
        migrator = new StoreMigrator( fs, pageCache, CONFIG, logService );
        countsMigrator = new CountsMigrator( fs, pageCache, CONFIG );
        migrator.moveMigratedFiles( migrationDir, storeDirectory, versionToMigrateFrom,
                upgradableDatabase.currentVersion() );
        countsMigrator.moveMigratedFiles( migrationDir, storeDirectory, versionToMigrateFrom,
                upgradableDatabase.currentVersion() );

        // THEN starting the new store should be successful
        StoreFactory storeFactory = new StoreFactory(
                storeDirectory, CONFIG, new DefaultIdGeneratorFactory( fs ), pageCache, fs,
                logService.getInternalLogProvider(), EmptyVersionContextSupplier.EMPTY );
        storeFactory.openAllNeoStores().close();
    }

    @Test
    public void shouldBeAbleToMigrateWithoutErrors() throws Exception
    {
        // GIVEN a legacy database
        File storeDirectory = directory.graphDbDir();
        File prepare = directory.directory( "prepare" );
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fs, storeDirectory, prepare );

        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        LogService logService = new SimpleLogService( logProvider, logProvider );
        PageCache pageCache = pageCacheRule.getPageCache( fs );

        LogTailScanner tailScanner = getTailScanner( storeDirectory );
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache, tailScanner );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory ).storeVersion();
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, CONFIG, logService );
        CountsMigrator countsMigrator = new CountsMigrator( fs, pageCache, CONFIG );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdirs( migrationDir );

        // WHEN migrating
        migrator.migrate( storeDirectory, migrationDir, progressMonitor.startSection( "section" ), versionToMigrateFrom,
                upgradableDatabase.currentVersion() );
        countsMigrator
                .migrate( storeDirectory, migrationDir, progressMonitor.startSection( "section" ), versionToMigrateFrom,
                        upgradableDatabase.currentVersion() );
        migrator.moveMigratedFiles( migrationDir, storeDirectory, versionToMigrateFrom,
                upgradableDatabase.currentVersion() );
        countsMigrator.moveMigratedFiles( migrationDir, storeDirectory, versionToMigrateFrom,
                upgradableDatabase.currentVersion() );

        // THEN starting the new store should be successful
        StoreFactory storeFactory = new StoreFactory(
                storeDirectory, CONFIG, new DefaultIdGeneratorFactory( fs ), pageCache, fs,
                logService.getInternalLogProvider(), EmptyVersionContextSupplier.EMPTY );
        storeFactory.openAllNeoStores().close();
        logProvider.assertNoLogCallContaining( "ERROR" );
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
        LogTailScanner tailScanner = getTailScanner( storeDirectory );
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache, tailScanner );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory ).storeVersion();
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, CONFIG, logService );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdirs( migrationDir );
        migrator.migrate( storeDirectory, migrationDir, progressMonitor.startSection( "section" ),
                versionToMigrateFrom, upgradableDatabase.currentVersion() );

        // WHEN simulating resuming the migration
        progressMonitor = new SilentMigrationProgressMonitor();
        CountsMigrator countsMigrator = new CountsMigrator( fs, pageCache, CONFIG );
        countsMigrator.migrate( storeDirectory, migrationDir, progressMonitor.startSection( "section" ),
                versionToMigrateFrom, upgradableDatabase.currentVersion() );
        migrator.moveMigratedFiles( migrationDir, storeDirectory, versionToMigrateFrom,
                upgradableDatabase.currentVersion() );
        countsMigrator.moveMigratedFiles( migrationDir, storeDirectory, versionToMigrateFrom,
                upgradableDatabase.currentVersion() );

        // THEN starting the new store should be successful
        StoreFactory storeFactory =
                new StoreFactory( storeDirectory, CONFIG, new DefaultIdGeneratorFactory( fs ), pageCache, fs,
                        logService.getInternalLogProvider(), EmptyVersionContextSupplier.EMPTY );
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
        LogTailScanner tailScanner = getTailScanner( storeDirectory );
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache, tailScanner );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory ).storeVersion();
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, CONFIG, logService );
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
        LogTailScanner tailScanner = getTailScanner( storeDirectory );
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache, tailScanner );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( storeDirectory ).storeVersion();
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        StoreMigrator migrator = new StoreMigrator( fs, pageCache, CONFIG, logService );
        File migrationDir = new File( storeDirectory, StoreUpgrader.MIGRATION_DIRECTORY );
        fs.mkdir( migrationDir );

        // when
        migrator.migrate( storeDirectory, migrationDir, progressMonitor.startSection( "section" ),
                versionToMigrateFrom, upgradableDatabase.currentVersion() );

        // then
        assertTrue( txIdComparator.apply( migrator.readLastTxInformation( migrationDir ) ) );
    }

    private UpgradableDatabase getUpgradableDatabase( PageCache pageCache, LogTailScanner tailScanner )
    {
        return new UpgradableDatabase( new StoreVersionCheck( pageCache ), selectFormat(), tailScanner );
    }

    private LogTailScanner getTailScanner( File storeDirectory ) throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDirectory, fs ).build();
        return new LogTailScanner( logFiles, new VersionAwareLogEntryReader<>(), monitors );
    }

    private RecordFormats selectFormat()
    {
        return Standard.LATEST_RECORD_FORMATS;
    }
}
