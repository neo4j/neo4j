/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.Monitor;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnableToUpgradeException;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.UpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v22.Legacy22Store;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allLegacyStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveNoTrailer;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.changeVersionNumber;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.checkNeoStoreHasLatestVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.containsAnyStoreFiles;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.isolatedMigrationDirectoryOf;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateAllFiles;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.verifyFilesHaveSameContent;
import static org.neo4j.kernel.impl.storemigration.UpgradeConfiguration.ALLOW_UPGRADE;

@RunWith(Parameterized.class)
public class StoreUpgraderTest
{
    private final String version;
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();

    public StoreUpgraderTest( String version )
    {
        this.version = version;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> versions()
    {
        return Arrays.asList(
                new Object[]{Legacy19Store.LEGACY_VERSION},
                new Object[]{Legacy20Store.LEGACY_VERSION},
                new Object[]{Legacy21Store.LEGACY_VERSION},
                new Object[]{Legacy22Store.LEGACY_VERSION}
        );
    }

    @Before
    public void prepareDb() throws IOException
    {
        dbDirectory = directory.directory( "db_" + version );
        File prepareDirectory = directory.directory("prepare_" + version);
        prepareSampleLegacyDatabase( version, fileSystem, dbDirectory, prepareDirectory );
    }

    @Test
    public void shouldUpgradeAnOldFormatStore() throws IOException, ConsistencyCheckIncompleteException
    {
        // Given
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );
        assertTrue( allLegacyStoreFilesHaveVersion( fileSystem, dbDirectory, version ) );

        // When
        newUpgrader( ALLOW_UPGRADE, pageCache ).migrateIfNeeded( dbDirectory, upgradableDatabase, schemaIndexProvider );

        // Then
        assertTrue( checkNeoStoreHasLatestVersion( check, dbDirectory ) );
        assertTrue( allStoreFilesHaveNoTrailer( fileSystem, dbDirectory ) );

        // We leave logical logs in place since the new version can read the old

        assertFalse( containsAnyStoreFiles( fileSystem, isolatedMigrationDirectoryOf( dbDirectory ) ) );
        // Since consistency checker is in read only mode we need to start/stop db to generate label scan store.
        startStopDatabase();
        assertConsistentStore( dbDirectory );
    }

    @Test
    public void shouldHaltUpgradeIfUpgradeConfigurationVetoesTheProcess()
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradeConfiguration vetoingUpgradeConfiguration = new UpgradeConfiguration()
        {
            @Override
            public void checkConfigurationAllowsAutomaticUpgrade()
            {
                throw new UpgradeNotAllowedByConfigurationException( "vetoed" );
            }
        };
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

        try
        {
            newUpgrader( vetoingUpgradeConfiguration, pageCache )
                    .migrateIfNeeded( dbDirectory, upgradableDatabase, schemaIndexProvider );
            fail( "Should throw exception" );
        }
        catch ( UpgradeNotAllowedByConfigurationException e )
        {
            // expected
        }
    }

    @Test
    public void shouldLeaveAllFilesUntouchedIfWrongVersionNumberFound()
            throws IOException, ConsistencyCheckIncompleteException
    {
        File comparisonDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName()
                + "shouldLeaveAllFilesUntouchedIfWrongVersionNumberFound-comparison" );

        changeVersionNumber( fileSystem, new File( dbDirectory, "neostore.nodestore.db" ), "v0.9.5" );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

        try
        {
            newUpgrader( ALLOW_UPGRADE, pageCache )
                    .migrateIfNeeded( dbDirectory, upgradableDatabase, schemaIndexProvider );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UnexpectedUpgradingStoreVersionException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fileSystem, comparisonDirectory, dbDirectory );
    }

    @Test
    public void shouldRefuseToUpgradeIfAnyOfTheStoresWeNotShutDownCleanly()
            throws IOException, ConsistencyCheckIncompleteException
    {
        File comparisonDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName()
                + "shouldRefuseToUpgradeIfAnyOfTheStoresWeNotShutDownCleanly-comparison" );

        truncateFile( fileSystem, new File( dbDirectory, "neostore.propertystore.db.index.keys" ),
                "StringPropertyStore v0.9.9" );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

        try
        {
            newUpgrader( ALLOW_UPGRADE, pageCache )
                    .migrateIfNeeded( dbDirectory, upgradableDatabase, schemaIndexProvider );
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
            throws IOException, ConsistencyCheckIncompleteException
    {
        File comparisonDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName()
                + "shouldRefuseToUpgradeIfAllOfTheStoresWeNotShutDownCleanly-comparison" );

        truncateAllFiles( fileSystem, dbDirectory, version );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

        try
        {
            newUpgrader( ALLOW_UPGRADE, pageCache )
                    .migrateIfNeeded( dbDirectory, upgradableDatabase, schemaIndexProvider );
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
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( dbDirectory );

        // GIVEN
        {
            StoreUpgrader upgrader = newUpgrader( ALLOW_UPGRADE, pageCache );
            String failureMessage = "Just failing";
            upgrader.addParticipant( participantThatWillFailWhenMoving( failureMessage ) );

            // WHEN
            try
            {
                upgrader.migrateIfNeeded( dbDirectory, upgradableDatabase, schemaIndexProvider );
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
            Monitor monitor = Mockito.mock( Monitor.class );
            StoreUpgrader upgrader = newUpgrader( monitor, pageCache );
            StoreMigrationParticipant observingParticipant = Mockito.mock( StoreMigrationParticipant.class );
            upgrader.addParticipant( observingParticipant );
            upgrader.migrateIfNeeded( dbDirectory, upgradableDatabase, schemaIndexProvider );

            // THEN
            verify( observingParticipant, Mockito.times( 0 ) ).migrate( any( File.class ), any( File.class ),
                    any( SchemaIndexProvider.class ), eq( versionToMigrateFrom ) );
            verify( observingParticipant, Mockito.times( 1 ) ).
                    moveMigratedFiles( any( File.class ), any( File.class ), eq( versionToMigrateFrom ) );

            verify( observingParticipant, Mockito.times( 1 ) ).cleanup( any( File.class ) );
            verify( monitor ).migrationCompleted();
        }
    }

    @Test
    public void upgradedNeoStoreShouldHaveNewUpgradeTimeAndUpgradeId() throws Exception
    {
        // Given
        fileSystem.deleteFile( new File( dbDirectory, StoreLogService.INTERNAL_LOG_NAME ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

        // When
        newUpgrader( ALLOW_UPGRADE, pageCache ).migrateIfNeeded( dbDirectory, upgradableDatabase, schemaIndexProvider );

        // Then
        StoreFactory storeFactory =
                new StoreFactory( fileSystem, dbDirectory, pageCache, NullLogProvider.getInstance() );
        NeoStores neoStores = storeFactory.openAllNeoStores();

        assertThat( neoStores.getMetaDataStore().getUpgradeTransaction(),
                equalTo( neoStores.getMetaDataStore().getLastCommittedTransaction() ) );
        assertThat( neoStores.getMetaDataStore().getUpgradeTime(), not( equalTo( MetaDataStore.FIELD_NOT_INITIALIZED ) ) );

        long minuteAgo = System.currentTimeMillis() - MINUTES.toMillis( 1 );
        assertThat( neoStores.getMetaDataStore().getUpgradeTime(), greaterThan( minuteAgo ) );
        neoStores.close();
    }

    @Test
    public void upgradeShouldNotLeaveLeftoverAndMigrationDirs() throws Exception
    {
        // Given
        fileSystem.deleteFile( new File( dbDirectory, StoreLogService.INTERNAL_LOG_NAME ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

        // When
        newUpgrader( ALLOW_UPGRADE, pageCache ).migrateIfNeeded( dbDirectory, upgradableDatabase, schemaIndexProvider );

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
        UpgradableDatabase upgradableDatabase =
                new UpgradableDatabase( new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        NullLogService instance = NullLogService.getInstance();
        StoreMigrator migrator = spy( new StoreMigrator(
                        progressMonitor, fileSystem, pageCache, new Config(), instance ) );
        newUpgrader( ALLOW_UPGRADE, migrator, StoreUpgrader.NO_MONITOR )
                .migrateIfNeeded( dbDirectory, upgradableDatabase, schemaIndexProvider );

        // Then
        assertThat( migrationHelperDirs(), is( emptyCollectionOf( File.class ) ) );
    }

    private StoreMigrationParticipant participantThatWillFailWhenMoving( final String failureMessage )
    {
        return new StoreMigrationParticipant()
        {
            @Override
            public void migrate( File storeDir, File migrationDir, SchemaIndexProvider schemaIndexProvider,
                    String versionToMigrateFrom ) throws IOException
            {  // Do nothing in particular
            }

            @Override
            public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom ) throws IOException
            {
                throw new IOException( failureMessage );
            }

            @Override
            public void rebuildCounts( File storeDir, String versionToMigrateFrom ) throws IOException
            {  // Do nothing in particular
            }

            @Override
            public void cleanup( File migrationDir ) throws IOException
            {  // Do nothing in particular
            }
        };
    }

    @Rule
    public final TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    private File dbDirectory;
    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
    private StoreVersionCheck check;

    private StoreUpgrader newUpgrader( UpgradeConfiguration upgradeConfig, PageCache pageCache )
    {
        check = new StoreVersionCheck( pageCache );
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        NullLogService instance = NullLogService.getInstance();
        StoreMigrator defaultMigrator =
                new StoreMigrator( progressMonitor, fileSystem, pageCache, new Config(), instance );
        return newUpgrader( upgradeConfig, defaultMigrator, StoreUpgrader.NO_MONITOR );
    }

    private StoreUpgrader newUpgrader( Monitor monitor, PageCache pageCache )
    {
        check = new StoreVersionCheck( pageCache );
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();
        NullLogService instance = NullLogService.getInstance();
        StoreMigrator defaultMigrator =
                new StoreMigrator( progressMonitor, fileSystem, pageCache, new Config(), instance );
        return newUpgrader( ALLOW_UPGRADE, defaultMigrator, monitor );
    }

    private StoreUpgrader newUpgrader( UpgradeConfiguration upgradeConfig, StoreMigrator migrator, Monitor monitor )
    {
        StoreUpgrader upgrader = new StoreUpgrader( upgradeConfig, fileSystem, monitor, NullLogProvider.getInstance() );
        upgrader.addParticipant( migrator );
        return upgrader;
    }

    private List<File> migrationHelperDirs()
    {
        File[] tmpDirs = dbDirectory.listFiles( new FilenameFilter()
        {
            @Override
            public boolean accept( File file, String name )
            {
                return file.isDirectory() &&
                        (name.equals( StoreUpgrader.MIGRATION_DIRECTORY ) ||
                                name.startsWith( StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY ));
            }
        } );
        assertNotNull( "Some IO errors occurred", tmpDirs );
        return Arrays.asList( tmpDirs );
    }

    private void startStopDatabase()
    {
        GraphDatabaseService databaseService = new TestGraphDatabaseFactory().newEmbeddedDatabase( dbDirectory );
        databaseService.shutdown();
    }
}
