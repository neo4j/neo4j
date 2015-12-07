/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.scan.InMemoryLabelScanStore;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.participant.AbstractStoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.participant.SchemaIndexMigrator;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.participant.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnableToUpgradeException;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v22.Legacy22Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v23.Legacy23Store;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

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
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.removeCheckPointFromTxLog;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateAllFiles;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.verifyFilesHaveSameContent;

@RunWith(Parameterized.class)
public class StoreUpgraderTest
{
    @Rule
    public final TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    private File dbDirectory;
    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
    private StoreVersionCheck check;
    private final String version;
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();
    private final LabelScanStoreProvider labelScanStoreProvider = new LabelScanStoreProvider( new
            InMemoryLabelScanStore(), 2 );

    private final Config allowMigrateConfig = new Config( MapUtil.stringMap( GraphDatabaseSettings
            .allow_store_upgrade.name(), "true" ) );

    public StoreUpgraderTest( String version )
    {
        this.version = version;
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> versions()
    {
        return Arrays.asList(
                new Object[]{Legacy19Store.LEGACY_VERSION},
                new Object[]{Legacy20Store.LEGACY_VERSION},
                new Object[]{Legacy21Store.LEGACY_VERSION},
                new Object[]{Legacy22Store.LEGACY_VERSION},
                new Object[]{Legacy23Store.LEGACY_VERSION}
        );
    }

    @Before
    public void prepareDb() throws IOException
    {
        dbDirectory = directory.directory( "db_" + version );
        File prepareDirectory = directory.directory( "prepare_" + version );
        prepareSampleLegacyDatabase( version, fileSystem, dbDirectory, prepareDirectory );
    }

    @Test
    public void shouldUpgradeAnOldFormatStore() throws IOException, ConsistencyCheckIncompleteException
    {
        // Given
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );
        assertEquals( !Legacy23Store.LEGACY_VERSION.equals( version ),
                allLegacyStoreFilesHaveVersion( fileSystem, dbDirectory, version ) );

        // When
        newUpgrader( upgradableDatabase, pageCache ).migrateIfNeeded( dbDirectory );

        // Then
        assertTrue( checkNeoStoreHasLatestVersion( check, dbDirectory ) );
        assertTrue( allStoreFilesHaveNoTrailer( fileSystem, dbDirectory ) );

        // We leave logical logs in place since the new version can read the old

        assertFalse( containsAnyStoreFiles( fileSystem, isolatedMigrationDirectoryOf( dbDirectory ) ) );
        assertConsistentStore( dbDirectory );
    }

    @Test
    public void shouldHaltUpgradeIfUpgradeConfigurationVetoesTheProcess()
    {
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        Config deniedMigrationConfig = new Config( MapUtil.stringMap( GraphDatabaseSettings
                .allow_store_upgrade.name(), "false" ) );

        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

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
    public void shouldLeaveAllFilesUntouchedIfWrongVersionNumberFound()
            throws IOException, ConsistencyCheckIncompleteException
    {
        Assume.assumeFalse( Legacy23Store.LEGACY_VERSION.equals( version ) );

        File comparisonDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName()
                                             + "shouldLeaveAllFilesUntouchedIfWrongVersionNumberFound-comparison" );

        changeVersionNumber( fileSystem, new File( dbDirectory, "neostore.nodestore.db" ), "v0.9.5" );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

        try
        {
            newUpgrader( upgradableDatabase, pageCache ).migrateIfNeeded( dbDirectory );
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
        makeDbNotCleanlyShutdown( false );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

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
            throws IOException, ConsistencyCheckIncompleteException
    {
        File comparisonDirectory = new File( "target/" + StoreUpgraderTest.class.getSimpleName()
                                             + "shouldRefuseToUpgradeIfAllOfTheStoresWeNotShutDownCleanly-comparison" );
        makeDbNotCleanlyShutdown( true );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

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
                new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

        String versionToMigrateFrom = upgradableDatabase.checkUpgradeable( dbDirectory );

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
                    any( MigrationProgressMonitor.Section.class ), eq( versionToMigrateFrom ) );
            verify( observingParticipant, Mockito.times( 1 ) ).
                    moveMigratedFiles( any( File.class ), any( File.class ), eq( versionToMigrateFrom ) );

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
                new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

        // When
        newUpgrader( upgradableDatabase, allowMigrateConfig, pageCache ).migrateIfNeeded( dbDirectory );

        // Then
        StoreFactory storeFactory =
                new StoreFactory( fileSystem, dbDirectory, pageCache, NullLogProvider.getInstance() );
        NeoStores neoStores = storeFactory.openAllNeoStores();

        assertThat( neoStores.getMetaDataStore().getUpgradeTransaction(),
                equalTo( neoStores.getMetaDataStore().getLastCommittedTransaction() ) );
        assertThat( neoStores.getMetaDataStore().getUpgradeTime(),
                not( equalTo( MetaDataStore.FIELD_NOT_INITIALIZED ) ) );

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
        UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fileSystem,
                new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );

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
                new StoreVersionCheck( pageCache ), new LegacyStoreVersionCheck( fileSystem ) );
        NullLogService instance = NullLogService.getInstance();
        StoreMigrator migrator = spy( new StoreMigrator(fileSystem, pageCache, new Config(), instance, schemaIndexProvider ) );
        StoreUpgrader storeUpgrader = newUpgrader( upgradableDatabase, pageCache );
        storeUpgrader.migrateIfNeeded( dbDirectory );

        // Then
        assertThat( migrationHelperDirs(), is( emptyCollectionOf( File.class ) ) );
    }

    private StoreMigrationParticipant participantThatWillFailWhenMoving( final String failureMessage )
    {
        return new AbstractStoreMigrationParticipant( "Failing" )
        {
            @Override
            public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom )
                    throws IOException
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
        check = new StoreVersionCheck( pageCache );
        SilentMigrationProgressMonitor progressMonitor = new SilentMigrationProgressMonitor();

        NullLogService instance = NullLogService.getInstance();
        StoreMigrator defaultMigrator = new StoreMigrator( fileSystem, pageCache, new Config(), instance,
                schemaIndexProvider );
        SchemaIndexMigrator indexMigrator =
                new SchemaIndexMigrator( fileSystem, schemaIndexProvider, labelScanStoreProvider );

        StoreUpgrader upgrader = new StoreUpgrader( upgradableDatabase, progressMonitor, config, fileSystem,
                NullLogProvider.getInstance() );
        upgrader.addParticipant( indexMigrator );
        upgrader.addParticipant( defaultMigrator );
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

    private void makeDbNotCleanlyShutdown( boolean truncateAll ) throws IOException
    {
        if ( Legacy23Store.LEGACY_VERSION.equals( version ) )
        {
            removeCheckPointFromTxLog( fileSystem, dbDirectory );
        }
        else
        {
            if ( truncateAll )
            {
                truncateAllFiles( fileSystem, dbDirectory, version );

            }
            else
            {
                File storeFile = new File( dbDirectory, "neostore.propertystore.db.index.keys" );
                truncateFile( fileSystem, storeFile, "StringPropertyStore " + version );
            }
        }
    }
}
