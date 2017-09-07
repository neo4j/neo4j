/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package upgrade;

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
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnableToUpgradeException;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.participant.AbstractStoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.participant.CountsMigrator;
import org.neo4j.kernel.impl.storemigration.participant.SchemaIndexMigrator;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.kernel.impl.transaction.log.LogTailScanner;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
    private final String version;
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();

    private final Config allowMigrateConfig = Config.defaults( GraphDatabaseSettings.allow_upgrade, "true" );

    public StoreUpgraderTest( String version )
    {
        this.version = version;
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<String> versions()
    {
        return Collections.singletonList( StandardV2_3.STORE_VERSION );
    }

    @Before
    public void prepareDb() throws IOException
    {
        dbDirectory = directory.directory( "db_" + version );
        File prepareDirectory = directory.directory( "prepare_" + version );
        prepareSampleDatabase( version, fileSystem, dbDirectory, prepareDirectory );
    }

    @Test
    public void shouldHaltUpgradeIfUpgradeConfigurationVetoesTheProcess() throws IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        Config deniedMigrationConfig = Config.defaults( GraphDatabaseSettings.allow_upgrade, "false" );

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
            verify( observingParticipant, Mockito.times( 0 ) ).migrate( any( File.class ), any( File.class ),
                    any( MigrationProgressMonitor.Section.class ), eq( versionToMigrateFrom ), eq( versionToMigrateTo ) );
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
        StoreFactory factory = new StoreFactory( dbDirectory, pageCache, fileSystem, NullLogProvider.getInstance() );
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
        logProvider.assertContainsLogCallContaining( "Count rebuilding" );
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

    protected void prepareSampleDatabase( String version, FileSystemAbstraction fileSystem, File dbDirectory,
            File databaseDirectory ) throws IOException
    {
        MigrationTestUtils.prepareSampleLegacyDatabase( version, fileSystem, dbDirectory, databaseDirectory );
    }

    private UpgradableDatabase getUpgradableDatabase( PageCache pageCache )
    {
        PhysicalLogFiles logFiles = new PhysicalLogFiles( dbDirectory, fileSystem );
        LogTailScanner tailScanner = new LogTailScanner( logFiles, fileSystem, new VersionAwareLogEntryReader<>() );
        return new UpgradableDatabase(
                new StoreVersionCheck( pageCache ),
                getRecordFormats(), tailScanner );
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
            throws IOException
    {
        return newUpgrader( upgradableDatabase, pageCache, config );
    }

    private StoreUpgrader newUpgrader( UpgradableDatabase upgradableDatabase, PageCache pageCache ) throws IOException
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
        SchemaIndexMigrator indexMigrator = new SchemaIndexMigrator( fileSystem, schemaIndexProvider );

        StoreUpgrader upgrader = new StoreUpgrader( upgradableDatabase, progressMonitor, config, fileSystem, pageCache,
                NullLogProvider.getInstance() );
        upgrader.addParticipant( indexMigrator );
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
