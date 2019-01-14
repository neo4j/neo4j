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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnableToUpgradeException;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.participant.AbstractStoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.participant.CountsMigrator;
import org.neo4j.kernel.impl.storemigration.participant.SchemaIndexMigrator;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.concurrent.TimeUnit.MINUTES;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.neo4j.graphdb.GraphDatabaseInternalLogIT.INTERNAL_LOG_FILE;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.removeCheckPointFromTxLog;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.verifyFilesHaveSameContent;

@RunWith( Parameterized.class )
public class StoreUpgraderTest
{
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( expectedException )
                                                .around( fileSystemRule )
                                                .around( pageCacheRule )
                                                .around( directory );

    private File dbDirectory;
    private final FileSystemAbstraction fileSystem = fileSystemRule.get();
    private final RecordFormats formats;
    private final IndexProvider indexProvider = new InMemoryIndexProvider();

    private final Config allowMigrateConfig = Config.defaults( GraphDatabaseSettings.allow_upgrade, "true" );

    public StoreUpgraderTest( RecordFormats formats )
    {
        this.formats = formats;
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<RecordFormats> versions()
    {
        return Collections.singletonList( StandardV2_3.RECORD_FORMATS );
    }

    @Before
    public void prepareDb() throws IOException
    {
        String version = formats.storeVersion();
        dbDirectory = directory.directory( "db_" + version );
        File prepareDirectory = directory.directory( "prepare_" + version );
        prepareSampleDatabase( version, fileSystem, dbDirectory, prepareDirectory );
    }

    @Test
    public void shouldHaltUpgradeIfUpgradeConfigurationVetoesTheProcess() throws IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        Config deniedMigrationConfig = Config.defaults( GraphDatabaseSettings.allow_upgrade, "false" );
        deniedMigrationConfig.augment( GraphDatabaseSettings.record_format, Standard.LATEST_NAME );

        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache );

        try
        {
            newUpgrader( upgradableDatabase, deniedMigrationConfig, pageCache ).migrateIfNeeded( dbDirectory );
            fail( "Should throw exception" );
        }
        catch ( UpgradeNotAllowedByConfigurationException e )
        {
            // expected
        }
    }

    @Test
    public void shouldRefuseToUpgradeIfAnyOfTheStoresWereNotShutDownCleanly()
            throws IOException
    {
        File comparisonDirectory = directory.directory(
                "shouldRefuseToUpgradeIfAnyOfTheStoresWereNotShutDownCleanly-comparison" );
        removeCheckPointFromTxLog( fileSystem, dbDirectory );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache );

        try
        {
            newUpgrader( upgradableDatabase, pageCache ).migrateIfNeeded( dbDirectory );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fileSystem, comparisonDirectory, dbDirectory );
    }

    @Test
    public void shouldRefuseToUpgradeIfAllOfTheStoresWereNotShutDownCleanly()
            throws IOException
    {
        File comparisonDirectory = directory.directory(
                "shouldRefuseToUpgradeIfAllOfTheStoresWereNotShutDownCleanly-comparison" );
        removeCheckPointFromTxLog( fileSystem, dbDirectory );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache );

        try
        {
            newUpgrader( upgradableDatabase, pageCache ).migrateIfNeeded( dbDirectory );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fileSystem, comparisonDirectory, dbDirectory );
    }

    @Test
    public void shouldContinueMovingFilesIfUpgradeCancelledWhileMoving() throws Exception
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache );

        String versionToMigrateTo = upgradableDatabase.currentVersion();
        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( dbDirectory ).storeVersion();

        // GIVEN
        {
            StoreUpgrader upgrader = newUpgrader( upgradableDatabase, allowMigrateConfig, pageCache );
            String failureMessage = "Just failing";
            upgrader.addParticipant( participantThatWillFailWhenMoving( failureMessage ) );

            // WHEN
            try
            {
                upgrader.migrateIfNeeded( dbDirectory );
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

            StoreUpgrader upgrader = newUpgrader( upgradableDatabase, pageCache );
            StoreMigrationParticipant observingParticipant = Mockito.mock( StoreMigrationParticipant.class );
            upgrader.addParticipant( observingParticipant );
            upgrader.migrateIfNeeded( dbDirectory );

            // THEN
            verify( observingParticipant, Mockito.never() ).migrate( any( File.class ), any( File.class ),
                    any( ProgressReporter.class ), eq( versionToMigrateFrom ), eq( versionToMigrateTo ) );
            verify( observingParticipant, Mockito.times( 1 ) ).
                    moveMigratedFiles( any( File.class ), any( File.class ), eq( versionToMigrateFrom ),
                            eq( versionToMigrateTo ) );

            verify( observingParticipant, Mockito.times( 1 ) ).cleanup( any( File.class ) );
        }
    }

    @Test
    public void upgradedNeoStoreShouldHaveNewUpgradeTimeAndUpgradeId() throws Exception
    {
        // Given
        fileSystem.deleteFile( new File( dbDirectory, INTERNAL_LOG_FILE ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache );

        // When
        newUpgrader( upgradableDatabase, allowMigrateConfig, pageCache ).migrateIfNeeded( dbDirectory );

        // Then
        StoreFactory factory = new StoreFactory(
                dbDirectory, allowMigrateConfig, new DefaultIdGeneratorFactory( fileSystem ), pageCache, fileSystem,
                NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
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
        fileSystem.deleteFile( new File( dbDirectory, INTERNAL_LOG_FILE ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache );

        // When
        newUpgrader( upgradableDatabase, allowMigrateConfig, pageCache ).migrateIfNeeded( dbDirectory );

        // Then
        assertThat( migrationHelperDirs(), is( emptyCollectionOf( File.class ) ) );
    }

    @Test
    public void upgradeShouldGiveProgressMonitorProgressMessages() throws Exception
    {
        // Given
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache );

        // When
        AssertableLogProvider logProvider = new AssertableLogProvider();
        newUpgrader( upgradableDatabase, pageCache, allowMigrateConfig,
                new VisibleMigrationProgressMonitor( logProvider.getLog( "test" ) ) ).migrateIfNeeded( dbDirectory );

        // Then
        logProvider.assertContainsLogCallContaining( "Store files" );
        logProvider.assertContainsLogCallContaining( "Indexes" );
        logProvider.assertContainsLogCallContaining( "Counts store" );
        logProvider.assertContainsLogCallContaining( "Successfully finished" );
    }

    @Test
    public void upgraderShouldCleanupLegacyLeftoverAndMigrationDirs() throws Exception
    {
        // Given
        fileSystem.deleteFile( new File( dbDirectory, INTERNAL_LOG_FILE ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_DIRECTORY ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY + "_1" ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY + "_2" ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY + "_42" ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );

        // When
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache );
        StoreUpgrader storeUpgrader = newUpgrader( upgradableDatabase, pageCache );
        storeUpgrader.migrateIfNeeded( dbDirectory );

        // Then
        assertThat( migrationHelperDirs(), is( emptyCollectionOf( File.class ) ) );
    }

    @Test
    public void notParticipatingParticipantsAreNotPartOfMigration() throws IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = getUpgradableDatabase( pageCache );
        StoreUpgrader storeUpgrader = newUpgrader( upgradableDatabase, pageCache );
        assertThat( storeUpgrader.getParticipants(), hasSize( 3 ) );
    }

    protected void prepareSampleDatabase( String version, FileSystemAbstraction fileSystem, File dbDirectory,
            File databaseDirectory ) throws IOException
    {
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fileSystem, dbDirectory, databaseDirectory );
    }

    private UpgradableDatabase getUpgradableDatabase( PageCache pageCache ) throws IOException
    {
        VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> entryReader = new VersionAwareLogEntryReader<>();
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( dbDirectory,fileSystem )
                .withLogEntryReader( entryReader ).build();
        LogTailScanner tailScanner = new LogTailScanner( logFiles, entryReader, new Monitors() );
        return new UpgradableDatabase( new StoreVersionCheck( pageCache ), getRecordFormats(), tailScanner );
    }

    private StoreMigrationParticipant participantThatWillFailWhenMoving( final String failureMessage )
    {
        return new AbstractStoreMigrationParticipant( "Failing" )
        {
            @Override
            public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom,
                    String versionToMigrateTo ) throws IOException
            {
                throw new IOException( failureMessage );
            }
        };
    }

    private StoreUpgrader newUpgrader( UpgradableDatabase upgradableDatabase, Config config, PageCache pageCache )
    {
        return newUpgrader( upgradableDatabase, pageCache, config );
    }

    private StoreUpgrader newUpgrader( UpgradableDatabase upgradableDatabase, PageCache pageCache )
    {
        return newUpgrader( upgradableDatabase, pageCache, allowMigrateConfig );
    }

    private StoreUpgrader newUpgrader( UpgradableDatabase upgradableDatabase, PageCache pageCache, Config config )
    {
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();

        return newUpgrader( upgradableDatabase, pageCache, config, progressMonitor );
    }

    private StoreUpgrader newUpgrader( UpgradableDatabase upgradableDatabase, PageCache pageCache, Config config,
            MigrationProgressMonitor progressMonitor )
    {
        NullLogService instance = NullLogService.getInstance();
        StoreMigrator defaultMigrator = new StoreMigrator( fileSystem, pageCache, getTuningConfig(), instance );
        CountsMigrator countsMigrator = new CountsMigrator( fileSystem, pageCache, getTuningConfig() );
        SchemaIndexMigrator indexMigrator = new SchemaIndexMigrator( fileSystem, indexProvider );

        StoreUpgrader upgrader = new StoreUpgrader( upgradableDatabase, progressMonitor, config, fileSystem, pageCache,
                NullLogProvider.getInstance() );
        upgrader.addParticipant( indexMigrator );
        upgrader.addParticipant( AbstractStoreMigrationParticipant.NOT_PARTICIPATING );
        upgrader.addParticipant( AbstractStoreMigrationParticipant.NOT_PARTICIPATING );
        upgrader.addParticipant( AbstractStoreMigrationParticipant.NOT_PARTICIPATING );
        upgrader.addParticipant( AbstractStoreMigrationParticipant.NOT_PARTICIPATING );
        upgrader.addParticipant( defaultMigrator );
        upgrader.addParticipant( countsMigrator );
        return upgrader;
    }

    private List<File> migrationHelperDirs()
    {
        File[] tmpDirs = dbDirectory.listFiles( ( file, name ) -> file.isDirectory() &&
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
}
