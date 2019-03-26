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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.LayoutConfig;
import org.neo4j.configuration.Settings;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnableToUpgradeException;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.migration.AbstractStoreMigrationParticipant;
import org.neo4j.storageengine.migration.MigrationProgressMonitor;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.storageengine.migration.UpgradeNotAllowedException;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.verifyFilesHaveSameContent;
import static org.neo4j.storageengine.migration.StoreMigrationParticipant.NOT_PARTICIPATING;

@RunWith( Parameterized.class )
public class StoreUpgraderTest
{
    private static final String INTERNAL_LOG_FILE = "debug.log";
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( expectedException )
                                                .around( fileSystemRule )
                                                .around( pageCacheRule )
                                                .around( directory );

    private DatabaseLayout databaseLayout;
    private final FileSystemAbstraction fileSystem = fileSystemRule.get();
    private JobScheduler jobScheduler;
    private final RecordFormats formats;

    private final Config allowMigrateConfig = Config.defaults( GraphDatabaseSettings.allow_upgrade, Settings.TRUE );
    private File prepareDatabaseDirectory;

    public StoreUpgraderTest( RecordFormats formats )
    {
        this.formats = formats;
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<RecordFormats> versions()
    {
        return Collections.singletonList( StandardV3_4.RECORD_FORMATS );
    }

    @Before
    public void prepareDb() throws IOException
    {
        jobScheduler = new ThreadPoolJobScheduler();
        String version = formats.storeVersion();
        databaseLayout = directory.databaseLayout( "db_" + version );
        prepareDatabaseDirectory = directory.directory( "prepare_" + version );
        prepareSampleDatabase( version, fileSystem, databaseLayout, prepareDatabaseDirectory );
    }

    @After
    public void tearDown() throws Exception
    {
        jobScheduler.close();
    }

    @Test
    public void shouldHaltUpgradeIfUpgradeConfigurationVetoesTheProcess() throws IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        Config deniedMigrationConfig = Config.defaults( GraphDatabaseSettings.allow_upgrade, "false" );
        deniedMigrationConfig.augment( GraphDatabaseSettings.record_format, Standard.LATEST_NAME );

        StoreVersionCheck check = getVersionCheck( pageCache );

        try
        {
            newUpgrader( check, deniedMigrationConfig, pageCache ).migrateIfNeeded( databaseLayout );
            fail( "Should throw exception" );
        }
        catch ( UpgradeNotAllowedException e )
        {
            // expected
        }
    }

    @Test
    public void shouldRefuseToUpgradeIfAnyOfTheStoresWereNotShutDownCleanly() throws IOException
    {
        File comparisonDirectory = directory.directory(
                "shouldRefuseToUpgradeIfAnyOfTheStoresWereNotShutDownCleanly-comparison" );
        removeCheckPointFromTxLog( fileSystem, databaseLayout.databaseDirectory() );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( databaseLayout.databaseDirectory(), comparisonDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreVersionCheck check = getVersionCheck( pageCache );

        try
        {
            newUpgrader( check, pageCache ).migrateIfNeeded( databaseLayout );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fileSystem, comparisonDirectory, databaseLayout.databaseDirectory() );
    }

    @Test
    public void shouldRefuseToUpgradeIfAllOfTheStoresWereNotShutDownCleanly() throws IOException
    {
        File comparisonDirectory = directory.directory(
                "shouldRefuseToUpgradeIfAllOfTheStoresWereNotShutDownCleanly-comparison" );
        removeCheckPointFromTxLog( fileSystem, databaseLayout.databaseDirectory() );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( databaseLayout.databaseDirectory(), comparisonDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreVersionCheck check = getVersionCheck( pageCache );

        try
        {
            newUpgrader( check, pageCache ).migrateIfNeeded( databaseLayout );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fileSystem, comparisonDirectory, databaseLayout.databaseDirectory() );
    }

    @Test
    public void shouldContinueMovingFilesIfUpgradeCancelledWhileMoving() throws Exception
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreVersionCheck check = getVersionCheck( pageCache );

        String versionToMigrateTo = check.configuredVersion();
        StoreVersionCheck.Result upgradeResult = check.checkUpgrade( check.configuredVersion() );
        assertTrue( upgradeResult.outcome.isSuccessful() );
        String versionToMigrateFrom = upgradeResult.actualVersion;

        // GIVEN
        {
            StoreUpgrader upgrader = newUpgrader( check, allowMigrateConfig, pageCache );
            String failureMessage = "Just failing";
            upgrader.addParticipant( participantThatWillFailWhenMoving( failureMessage ) );

            // WHEN
            try
            {
                upgrader.migrateIfNeeded( databaseLayout );
                fail( "should have thrown" );
            }
            catch ( UnableToUpgradeException e )
            {   // THEN
                assertTrue( e.getCause() instanceof IOException );
                assertEquals( failureMessage, e.getCause().getMessage() );
            }
        }

        // AND WHEN
        {

            StoreUpgrader upgrader = newUpgrader( check, pageCache );
            StoreMigrationParticipant observingParticipant = Mockito.mock( StoreMigrationParticipant.class );
            upgrader.addParticipant( observingParticipant );
            upgrader.migrateIfNeeded( databaseLayout );

            // THEN
            verify( observingParticipant, Mockito.never() ).migrate( any( DatabaseLayout.class ), any( DatabaseLayout.class ),
                    any( ProgressReporter.class ), eq( versionToMigrateFrom ), eq( versionToMigrateTo ) );
            verify( observingParticipant ).
                    moveMigratedFiles( any( DatabaseLayout.class ), any( DatabaseLayout.class ), eq( versionToMigrateFrom ),
                            eq( versionToMigrateTo ) );

            verify( observingParticipant ).cleanup( any( DatabaseLayout.class ) );
        }
    }

    @Test
    public void upgradedNeoStoreShouldHaveNewUpgradeTimeAndUpgradeId() throws Exception
    {
        // Given
        fileSystem.deleteFile( databaseLayout.file( INTERNAL_LOG_FILE ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreVersionCheck check = getVersionCheck( pageCache );

        // When
        newUpgrader( check, allowMigrateConfig, pageCache ).migrateIfNeeded( databaseLayout );

        // Then
        StoreFactory factory = new StoreFactory( databaseLayout, allowMigrateConfig, new DefaultIdGeneratorFactory( fileSystem ), pageCache, fileSystem,
                NullLogProvider.getInstance() );
        try ( NeoStores neoStores = factory.openAllNeoStores() )
        {
            assertThat( neoStores.getMetaDataStore().getUpgradeTransaction(),
                    equalTo( neoStores.getMetaDataStore().getLastCommittedTransaction() ) );
            assertThat( neoStores.getMetaDataStore().getUpgradeTime(),
                    not( equalTo( MetaDataStore.FIELD_NOT_INITIALIZED ) ) );

            long minuteAgo = System.currentTimeMillis() - MINUTES.toMillis( 1 );
            assertThat( neoStores.getMetaDataStore().getUpgradeTime(), greaterThan( minuteAgo ) );
        }
    }

    @Test
    public void upgradeShouldNotLeaveLeftoverAndMigrationDirs() throws Exception
    {
        // Given
        fileSystem.deleteFile( databaseLayout.file( INTERNAL_LOG_FILE ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreVersionCheck check = getVersionCheck( pageCache );

        // When
        newUpgrader( check, allowMigrateConfig, pageCache ).migrateIfNeeded( databaseLayout );

        // Then
        assertThat( migrationHelperDirs(), is( emptyCollectionOf( File.class ) ) );
    }

    @Test
    public void upgradeShouldGiveProgressMonitorProgressMessages() throws Exception
    {
        // Given
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreVersionCheck check = getVersionCheck( pageCache );

        // When
        AssertableLogProvider logProvider = new AssertableLogProvider();
        newUpgrader( check, pageCache, allowMigrateConfig,
                new VisibleMigrationProgressMonitor( logProvider.getLog( "test" ) ) ).migrateIfNeeded( databaseLayout );

        // Then
        logProvider.assertContainsLogCallContaining( "Store files" );
        logProvider.assertContainsLogCallContaining( "Indexes" );
        logProvider.assertContainsLogCallContaining( "Successfully finished" );
    }

    @Test
    public void upgraderShouldCleanupLegacyLeftoverAndMigrationDirs() throws Exception
    {
        // Given
        fileSystem.deleteFile( databaseLayout.file( INTERNAL_LOG_FILE ) );
        fileSystem.mkdir( databaseLayout.file( StoreUpgrader.MIGRATION_DIRECTORY ) );
        fileSystem.mkdir( databaseLayout.file( StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY ) );
        fileSystem.mkdir( databaseLayout.file( StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY + "_1" ) );
        fileSystem.mkdir( databaseLayout.file( StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY + "_2" ) );
        fileSystem.mkdir( databaseLayout.file( StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY + "_42" ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );

        // When
        StoreVersionCheck check = getVersionCheck( pageCache );
        StoreUpgrader storeUpgrader = newUpgrader( check, pageCache );
        storeUpgrader.migrateIfNeeded( databaseLayout );

        // Then
        assertThat( migrationHelperDirs(), is( emptyCollectionOf( File.class ) ) );
    }

    @Test
    public void upgradeFailsIfMigrationIsNotAllowed()
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreVersionCheck check = getVersionCheck( pageCache );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        assertThrows( UpgradeNotAllowedException.class, () -> newUpgrader( check, pageCache, Config.defaults(),
                new VisibleMigrationProgressMonitor( logProvider.getLog( "test" ) ) ).migrateIfNeeded( databaseLayout ) );
    }

    @Test
    public void upgradeMoveTransactionLogs() throws IOException
    {
        File txRoot = directory.directory( "customTxRoot" );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreVersionCheck check = getVersionCheck( pageCache );

        Config config = Config.builder().withSettings( allowMigrateConfig.getRaw() )
                    .withSetting( GraphDatabaseSettings.transaction_logs_root_path, txRoot.getAbsolutePath() ).build();
        DatabaseLayout migrationLayout = DatabaseLayout.of( databaseLayout.databaseDirectory(), LayoutConfig.of( config ) );
        newUpgrader( check, pageCache, config, new VisibleMigrationProgressMonitor( logProvider.getLog( "test" ) ) )
                .migrateIfNeeded( migrationLayout );

        logProvider.assertContainsLogCallContaining( "Starting transaction logs migration." );
        logProvider.assertContainsLogCallContaining( "Transaction logs migration completed." );
        assertThat( getLogFiles( migrationLayout.databaseDirectory() ), emptyArray() );
        File databaseTransactionLogsHome = new File( txRoot, migrationLayout.getDatabaseName() );
        assertTrue( fileSystem.fileExists( databaseTransactionLogsHome ) );

        Set<String> logFileNames = getLogFileNames( databaseTransactionLogsHome );
        assertThat( logFileNames, not( empty() ) );
        assertEquals( getLogFileNames( prepareDatabaseDirectory ), logFileNames );
    }

    @Test
    public void failToMoveTransactionLogsIfTheyAlreadyExist() throws IOException
    {
        File txRoot = directory.directory( "customTxRoot" );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreVersionCheck check = getVersionCheck( pageCache );

        Config config = Config.builder().withSettings( allowMigrateConfig.getRaw() )
                .withSetting( GraphDatabaseSettings.transaction_logs_root_path, txRoot.getAbsolutePath() ).build();
        DatabaseLayout migrationLayout = DatabaseLayout.of( databaseLayout.databaseDirectory(), LayoutConfig.of( config ) );

        File databaseTransactionLogsHome = new File( txRoot, migrationLayout.getDatabaseName() );
        assertTrue( fileSystem.mkdir( databaseTransactionLogsHome ) );
        createDummyTxLogFiles( databaseTransactionLogsHome );

        try
        {
            newUpgrader( check, pageCache, config, new VisibleMigrationProgressMonitor( logProvider.getLog( "test" ) ) )
                    .migrateIfNeeded( migrationLayout );
            fail( "Should fail during transaction logs move" );
        }
        catch ( StoreUpgrader.TransactionLogsRelocationException e )
        {
            // expected
        }
    }

    @Test
    public void notParticipatingParticipantsAreNotPartOfMigration() throws IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        StoreVersionCheck check = getVersionCheck( pageCache );
        StoreUpgrader storeUpgrader = newUpgrader( check, pageCache );
        assertThat( storeUpgrader.getParticipants(), hasSize( 2 ) );
    }

    private void createDummyTxLogFiles( File databaseTransactionLogsHome ) throws IOException
    {
        Set<String> preparedLogFiles = getLogFileNames( prepareDatabaseDirectory );
        assertThat( preparedLogFiles, not( empty() ) );
        for ( String preparedLogFile : preparedLogFiles )
        {
            fileSystem.write( new File( databaseTransactionLogsHome, preparedLogFile ) ).close();
        }
    }

    private File[] getLogFiles( File directory ) throws IOException
    {
        return LogFilesBuilder.logFilesBasedOnlyBuilder( directory, fileSystem ).build().logFiles();
    }

    private Set<String> getLogFileNames( File directory ) throws IOException
    {
        return Arrays.stream( LogFilesBuilder.logFilesBasedOnlyBuilder( directory, fileSystem )
                .build()
                .logFiles() )
                .map( File::getName ).collect( Collectors.toSet() );
    }

    protected void prepareSampleDatabase( String version, FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout,
            File databaseDirectory ) throws IOException
    {
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fileSystem, databaseLayout.databaseDirectory(), databaseDirectory );
    }

    private StoreVersionCheck getVersionCheck( PageCache pageCache )
    {
        return new RecordStoreVersionCheck( fileSystem, pageCache, databaseLayout, NullLogProvider.getInstance(), getTuningConfig() );
    }

    private StoreMigrationParticipant participantThatWillFailWhenMoving( final String failureMessage )
    {
        return new AbstractStoreMigrationParticipant( "Failing" )
        {
            @Override
            public void migrate( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, ProgressReporter progress, String versionToMigrateFrom,
                String versionToMigrateTo )
            {
                // nop
            }

            @Override
            public void moveMigratedFiles( DatabaseLayout migrationLayout, DatabaseLayout directoryLayout, String versionToUpgradeFrom,
                    String versionToMigrateTo ) throws IOException
            {
                throw new IOException( failureMessage );
            }

            @Override
            public void cleanup( DatabaseLayout migrationLayout )
            {
                // nop
            }
        };
    }

    private StoreUpgrader newUpgrader( StoreVersionCheck storeVersionCheck, Config config, PageCache pageCache ) throws IOException
    {
        return newUpgrader( storeVersionCheck, pageCache, config );
    }

    private StoreUpgrader newUpgrader( StoreVersionCheck storeVersionCheck, PageCache pageCache ) throws IOException
    {
        return newUpgrader( storeVersionCheck, pageCache, allowMigrateConfig );
    }

    private StoreUpgrader newUpgrader( StoreVersionCheck storeVersionCheck, PageCache pageCache, Config config ) throws IOException
    {
        return newUpgrader( storeVersionCheck, pageCache, config, MigrationProgressMonitor.SILENT );
    }

    private StoreUpgrader newUpgrader( StoreVersionCheck storeVersionCheck, PageCache pageCache, Config config,
            MigrationProgressMonitor progressMonitor ) throws IOException
    {
        NullLogService instance = NullLogService.getInstance();
        RecordStorageMigrator defaultMigrator = new RecordStorageMigrator( fileSystem, pageCache, getTuningConfig(), instance, jobScheduler );
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine();
        SchemaIndexMigrator indexMigrator = new SchemaIndexMigrator( fileSystem, IndexProvider.EMPTY, storageEngineFactory );

        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.databaseDirectory(), fileSystem )
                .withLogEntryReader( new VersionAwareLogEntryReader(  ) ).build();
        LogTailScanner logTailScanner = new LogTailScanner( logFiles, new VersionAwareLogEntryReader<>(), new Monitors() );
        StoreUpgrader upgrader = new StoreUpgrader( storeVersionCheck, progressMonitor, config, fileSystem, NullLogProvider.getInstance(),
                logTailScanner, new LegacyTransactionLogsLocator( config, databaseLayout ) );
        upgrader.addParticipant( indexMigrator );
        upgrader.addParticipant( NOT_PARTICIPATING );
        upgrader.addParticipant( NOT_PARTICIPATING );
        upgrader.addParticipant( NOT_PARTICIPATING );
        upgrader.addParticipant( NOT_PARTICIPATING );
        upgrader.addParticipant( defaultMigrator );
        return upgrader;
    }

    private List<File> migrationHelperDirs()
    {
        File[] tmpDirs = databaseLayout.listDatabaseFiles( ( file, name ) -> file.isDirectory() &&
                                                          (name.equals( StoreUpgrader.MIGRATION_DIRECTORY ) ||
                name.startsWith( StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY )) );
        assertNotNull( "Some IO errors occurred", tmpDirs );
        return Arrays.asList( tmpDirs );
    }

    private Config getTuningConfig()
    {
        return Config.defaults( GraphDatabaseSettings.record_format, getRecordFormatsName() );
    }

    protected RecordFormats getRecordFormats()
    {
        return Standard.LATEST_RECORD_FORMATS;
    }

    protected String getRecordFormatsName()
    {
        return Standard.LATEST_NAME;
    }

    public static void removeCheckPointFromTxLog( FileSystemAbstraction fileSystem, File databaseDirectory )
            throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseDirectory, fileSystem ).build();
        LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        LogTailScanner tailScanner = new LogTailScanner( logFiles, logEntryReader, new Monitors() );
        LogTailScanner.LogTailInformation logTailInformation = tailScanner.getTailInformation();

        if ( logTailInformation.commitsAfterLastCheckpoint() )
        {
            // done already
            return;
        }

        // let's assume there is at least a checkpoint
        assertNotNull( logTailInformation.lastCheckPoint );

        LogPosition logPosition = logTailInformation.lastCheckPoint.getLogPosition();
        File logFile = logFiles.getLogFileForVersion( logPosition.getLogVersion() );
        long byteOffset = logPosition.getByteOffset();
        fileSystem.truncate( logFile, byteOffset );
    }
}
