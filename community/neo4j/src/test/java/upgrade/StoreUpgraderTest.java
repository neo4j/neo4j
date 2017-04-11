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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnableToUpgradeException;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogFilenames;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.participant.AbstractStoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.participant.SchemaIndexMigrator;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.NeoStoreDataSourceRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.removeCheckPointFromTxLog;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.verifyFilesHaveSameContent;

@RunWith(Parameterized.class)
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
    private LabelScanStoreProvider labelScanStoreProvider;

    private final Config allowMigrateConfig = Config.embeddedDefaults( MapUtil.stringMap( GraphDatabaseSettings
            .allow_store_upgrade.name(), "true" ) );

    public StoreUpgraderTest( String version )
    {
        this.version = version;
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<String> versions()
    {
        return Arrays.asList(
                StandardV2_3.STORE_VERSION
        );
    }

    @Before
    public void prepareDb() throws IOException
    {
        dbDirectory = directory.directory( "db_" + version );
        labelScanStoreProvider = NeoStoreDataSourceRule.nativeLabelScanStoreProvider( dbDirectory, fileSystem,
                pageCacheRule.getPageCache( fileSystem ) );
        File prepareDirectory = directory.directory( "prepare_" + version );
        prepareSampleDatabase( version, fileSystem, dbDirectory, prepareDirectory );
    }

    @Test
    public void failMigrationWhenFailDuringTransactionInformationRetrieval() throws IOException
    {
        File storeDirectory = directory.graphDbDir();
        File prepare = directory.directory( "prepare" );
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( fileSystem )
        {
            @Override
            public File[] listFiles( File directory, FilenameFilter filter )
            {
                if ( filter == LegacyLogFilenames.versionedLegacyLogFilesFilter )
                {
                    sneakyThrow( new IOException( "Enforced IO Exception Fail to open file" ) );
                }
                return super.listFiles( directory, filter );
            }
        };

        prepareSampleDatabase( version, fs, storeDirectory, prepare );
        // and a state of the migration saying that it has done the actual migration
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        // remove metadata store record to force tx info lookup in tx logs
        MetaDataStore.setRecord( pageCache, new File( storeDirectory, MetaDataStore.DEFAULT_NAME),
                MetaDataStore.Position.LAST_TRANSACTION_COMMIT_TIMESTAMP, MetaDataRecordFormat.FIELD_NOT_PRESENT );

        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fs, new StoreVersionCheck( pageCache ),
                getRecordFormats() )
        {
            @Override
            public RecordFormats checkUpgradeable( File storeDirectory )
            {
                return getRecordFormats();
            }
        };
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();

        StoreMigrator defaultMigrator = new StoreMigrator( fs, pageCache, getTuningConfig(), NullLogService.getInstance()

        );
        StoreUpgrader upgrader = new StoreUpgrader( upgradableDatabase, progressMonitor, allowMigrateConfig, fs,
                pageCache, NullLogProvider.getInstance() );
        upgrader.addParticipant( defaultMigrator );

        expectedException.expect( UnableToUpgradeException.class );
        expectedException.expectCause( instanceOf( IOException.class ) );
        expectedException.expectCause( hasMessage( containsString( "Enforced IO Exception Fail to open file" ) ) );

        upgrader.migrateIfNeeded( storeDirectory );
    }

    @Test
    public void shouldHaltUpgradeIfUpgradeConfigurationVetoesTheProcess() throws IOException
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        Config deniedMigrationConfig = Config.embeddedDefaults( MapUtil.stringMap( GraphDatabaseSettings
                .allow_store_upgrade.name(), "false" ) );

        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ),
                getRecordFormats() );

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
        File comparisonDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() +
                                             "shouldRefuseToUpgradeIfAnyOfTheStoresWereNotShutDownCleanly-comparison" );
        removeCheckPointFromTxLog( fileSystem, dbDirectory );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ),
                getRecordFormats() );

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
        File comparisonDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName() +
                                            "shouldRefuseToUpgradeIfAllOfTheStoresWereNotShutDownCleanly-comparison" );
        removeCheckPointFromTxLog( fileSystem, dbDirectory );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ),
                getRecordFormats() );

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
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ),
                getRecordFormats() );

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
        fileSystem.deleteFile( new File( dbDirectory, StoreLogService.INTERNAL_LOG_NAME ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ),
                getRecordFormats() );

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
        fileSystem.deleteFile( new File( dbDirectory, StoreLogService.INTERNAL_LOG_NAME ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ),
                getRecordFormats() );

        // When
        newUpgrader( upgradableDatabase, allowMigrateConfig, pageCache ).migrateIfNeeded( dbDirectory );

        // Then
        assertThat( migrationHelperDirs(), is( emptyCollectionOf( File.class ) ) );
    }

    @Test
    public void upgraderShouldCleanupLegacyLeftoverAndMigrationDirs() throws Exception
    {
        // Given
        fileSystem.deleteFile( new File( dbDirectory, StoreLogService.INTERNAL_LOG_NAME ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_DIRECTORY ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY + "_1" ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY + "_2" ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY + "_42" ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );

        // When
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ),
                getRecordFormats() );
        StoreUpgrader storeUpgrader = newUpgrader( upgradableDatabase, pageCache );
        storeUpgrader.migrateIfNeeded( dbDirectory );

        // Then
        assertThat( migrationHelperDirs(), is( emptyCollectionOf( File.class ) ) );
    }

    protected void prepareSampleDatabase( String version, FileSystemAbstraction fileSystem, File dbDirectory,
            File databaseDirectory ) throws IOException
    {
        prepareSampleLegacyDatabase( version, fileSystem, dbDirectory, databaseDirectory );
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
            throws IOException
    {
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();

        NullLogService instance = NullLogService.getInstance();
        StoreMigrator defaultMigrator = new StoreMigrator( fileSystem, pageCache, getTuningConfig(), instance
        );
        SchemaIndexMigrator indexMigrator =
                new SchemaIndexMigrator( fileSystem, schemaIndexProvider, labelScanStoreProvider );

        StoreUpgrader upgrader = new StoreUpgrader( upgradableDatabase, progressMonitor, config, fileSystem, pageCache,
                NullLogProvider.getInstance() );
        upgrader.addParticipant( indexMigrator );
        upgrader.addParticipant( defaultMigrator );
        return upgrader;
    }

    private static <T extends Throwable> void sneakyThrow( Throwable throwable ) throws T
    {
        throw (T) throwable;
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
        return Config.embeddedDefaults( MapUtil.stringMap( GraphDatabaseSettings.record_format.name(), getRecordFormatsName() ) );
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
