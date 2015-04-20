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
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.Monitor;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnableToUpgradeException;
import org.neo4j.kernel.impl.storemigration.UpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.monitoring.Monitors;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.changeVersionNumber;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.containsAnyStoreFiles;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.isolatedMigrationDirectoryOf;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateAllFiles;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.verifyFilesHaveSameContent;
import static org.neo4j.kernel.impl.storemigration.UpgradeConfiguration.ALLOW_UPGRADE;
import static org.neo4j.kernel.logging.DevNullLoggingService.DEV_NULL;

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
                new Object[]{Legacy21Store.LEGACY_VERSION}
        );
    }

    @Before
    public void prepareDb() throws IOException
    {
        dbDirectory = new File( directory.directory(), version );
        prepareSampleLegacyDatabase( version, fileSystem, dbDirectory );
    }

    @Test
    public void shouldUpgradeAnOldFormatStore() throws IOException, ConsistencyCheckIncompleteException
    {
        // Given
        assertTrue( allStoreFilesHaveVersion( fileSystem, dbDirectory, version ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );

        // When
        newUpgrader( ALLOW_UPGRADE ).migrateIfNeeded( dbDirectory, schemaIndexProvider, pageCache );

        // Then
        assertTrue( allStoreFilesHaveVersion( fileSystem, dbDirectory, ALL_STORES_VERSION ) );

        // We leave logical logs in place since the new version can read the old

        assertFalse( containsAnyStoreFiles( fileSystem, isolatedMigrationDirectoryOf( dbDirectory ) ) );
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

        try
        {
            newUpgrader( vetoingUpgradeConfiguration ).migrateIfNeeded( dbDirectory, schemaIndexProvider, pageCache );
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

        try
        {
            newUpgrader( ALLOW_UPGRADE ).migrateIfNeeded( dbDirectory, schemaIndexProvider, pageCache );
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

        try
        {
            newUpgrader( ALLOW_UPGRADE ).migrateIfNeeded( dbDirectory, schemaIndexProvider, pageCache );
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

        try
        {
            newUpgrader( ALLOW_UPGRADE ).migrateIfNeeded( dbDirectory, schemaIndexProvider, pageCache );
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
        // GIVEN
        StoreUpgrader upgrader = newUpgrader( ALLOW_UPGRADE );
        String failureMessage = "Just failing";
        upgrader.addParticipant( participantThatWillFailWhenMoving( failureMessage ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );

        // WHEN
        try
        {
            upgrader.migrateIfNeeded( dbDirectory, schemaIndexProvider, pageCache );
        }
        catch ( UnableToUpgradeException e )
        {   // THEN
            assertTrue( e.getCause() instanceof IOException );
            assertEquals( failureMessage, e.getCause().getMessage() );
        }

        // AND WHEN
        Monitor monitor = Mockito.mock( Monitor.class );
        upgrader = newUpgrader( monitor );
        StoreMigrationParticipant observingParticipant = Mockito.mock( StoreMigrationParticipant.class );
        when( observingParticipant.needsMigration( any( File.class ) ) ).thenReturn( true );
        upgrader.addParticipant( observingParticipant );
        upgrader.migrateIfNeeded( dbDirectory, schemaIndexProvider, pageCache );

        // THEN
        verify( observingParticipant, Mockito.times( 0 ) ).migrate(
                any( File.class ), any( File.class ), any( SchemaIndexProvider.class ), any( PageCache.class ) );
        verify( observingParticipant, Mockito.times( 1 ) ).moveMigratedFiles( any( File.class ), any( File.class ) );
        verify( observingParticipant, Mockito.times( 1 ) ).cleanup( any( File.class ) );
        verify( monitor ).migrationCompleted();
    }

    @Test
    public void upgradedNeoStoreShouldHaveNewUpgradeTimeAndUpgradeId() throws Exception
    {
        // Given
        fileSystem.deleteFile( new File( dbDirectory, StringLogger.DEFAULT_NAME ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );

        // When
        newUpgrader( ALLOW_UPGRADE ).migrateIfNeeded( dbDirectory, schemaIndexProvider, pageCache );

        // Then
        NeoStore neoStore = new StoreFactory( fileSystem, dbDirectory, pageCache,
                StringLogger.DEV_NULL, mock( Monitors.class ) ).newNeoStore( false );

        assertThat( neoStore.getUpgradeTransaction(), equalTo( neoStore.getLastCommittedTransaction() ) );
        assertThat( neoStore.getUpgradeTime(), not( equalTo( NeoStore.FIELD_NOT_INITIALIZED ) ) );

        long minuteAgo = System.currentTimeMillis() - MINUTES.toMillis( 1 );
        assertThat( neoStore.getUpgradeTime(), greaterThan( minuteAgo ) );
        neoStore.close();
    }

    @Test
    public void upgradeShouldNotLeaveLeftoverAndMigrationDirs() throws Exception
    {
        // Given
        fileSystem.deleteFile( new File( dbDirectory, StringLogger.DEFAULT_NAME ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );

        // When
        newUpgrader( ALLOW_UPGRADE ).migrateIfNeeded( dbDirectory, schemaIndexProvider, pageCache );

        // Then
        assertThat( migrationHelperDirs(), is( emptyCollectionOf( File.class ) ) );
    }

    @Test
    public void upgraderShouldCleanupLegacyLeftoverAndMigrationDirs() throws Exception
    {
        // Given
        fileSystem.deleteFile( new File( dbDirectory, StringLogger.DEFAULT_NAME ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_DIRECTORY ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY + "_1" ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY + "_2" ) );
        fileSystem.mkdir( new File( dbDirectory, StoreUpgrader.MIGRATION_LEFT_OVERS_DIRECTORY + "_42" ) );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystem );

        // When
        StoreMigrator migrator = spy( new StoreMigrator( new SilentMigrationProgressMonitor(), fileSystem, DEV_NULL ) );
        when( migrator.needsMigration( dbDirectory ) ).thenReturn( false );
        newUpgrader( ALLOW_UPGRADE, migrator, StoreUpgrader.NO_MONITOR ).migrateIfNeeded( dbDirectory,
                schemaIndexProvider, pageCache );

        // Then
        assertThat( migrationHelperDirs(), is( emptyCollectionOf( File.class ) ) );
    }

    private StoreMigrationParticipant participantThatWillFailWhenMoving( final String failureMessage )
    {
        return new StoreMigrationParticipant()
        {
            @Override
            public boolean needsMigration( File storeDir ) throws IOException
            {
                return true;
            }

            @Override
            public void migrate( File storeDir, File migrationDir, SchemaIndexProvider schemaIndexProvider,
                                 PageCache pageCache ) throws IOException
            {  // Do nothing in particular
            }

            @Override
            public void moveMigratedFiles( File migrationDir, File storeDir ) throws IOException
            {
                throw new IOException( failureMessage );
            }

            @Override
            public void close()
            {  // Do nothing in particular
            }

            @Override
            public void cleanup( File migrationDir ) throws IOException
            {  // Do nothing in particular
            }
        };
    }

    @Rule
    public final TestDirectory directory = TargetDirectory.forTest( getClass() ).testDirectory();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    private File dbDirectory;
    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

    private StoreUpgrader newUpgrader( UpgradeConfiguration upgradeConfig )
    {
        StoreMigrator defaultMigrator = new StoreMigrator(
                new SilentMigrationProgressMonitor(), fileSystem,
                DEV_NULL );
        return newUpgrader( upgradeConfig, defaultMigrator, StoreUpgrader.NO_MONITOR );
    }

    private StoreUpgrader newUpgrader( Monitor monitor )
    {
        StoreMigrator defaultMigrator = new StoreMigrator(
                new SilentMigrationProgressMonitor(), fileSystem,
                DEV_NULL );
        return newUpgrader( ALLOW_UPGRADE, defaultMigrator, monitor );
    }

    private StoreUpgrader newUpgrader( UpgradeConfiguration upgradeConfig, StoreMigrator migrator, Monitor monitor )
    {
        DevNullLoggingService logging = new DevNullLoggingService();
        StoreUpgrader upgrader = new StoreUpgrader( upgradeConfig, fileSystem, monitor, logging );
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
}
