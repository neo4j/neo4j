/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Matchers;
import org.mockito.Mockito;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
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
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.changeVersionNumber;
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
    @Rule
    public final TestDirectory directory = TargetDirectory.forTest( getClass() ).testDirectory();

    private final String version;
    private File dbDirectory;
    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();

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
        assertTrue( allStoreFilesHaveVersion( fileSystem, dbDirectory, version ) );

        newUpgrader( ALLOW_UPGRADE, new StoreMigrator( new SilentMigrationProgressMonitor(), fileSystem ) )
                .migrateIfNeeded( dbDirectory );

        assertTrue( allStoreFilesHaveVersion( fileSystem, dbDirectory, ALL_STORES_VERSION ) );

        // We leave logical logs in place since the new version can read the old

        assertFalse( containsAnyStoreFiles( fileSystem, isolatedMigrationDirectoryOf( dbDirectory ) ) );
        assertConsistentStore( dbDirectory );
    }

    @Test
    public void shouldBackupOriginalStoreEvenIfMessagesLogIsMissing() throws ConsistencyCheckIncompleteException
    {
        // given
        fileSystem.deleteFile( new File( dbDirectory, StringLogger.DEFAULT_NAME ) );

        // when
        newUpgrader( ALLOW_UPGRADE, new StoreMigrator( new SilentMigrationProgressMonitor(), fileSystem ) )
                .migrateIfNeeded( dbDirectory );

        // then
        File backupDirectory = new File( dbDirectory, "upgrade_backup" );
        assertFalse( fileSystem.fileExists( new File( dbDirectory, StringLogger.DEFAULT_NAME ) ) );
        assertFalse( fileSystem.fileExists( new File( backupDirectory, StringLogger.DEFAULT_NAME ) ) );
        assertConsistentStore( dbDirectory );
    }

    @Test
    public void shouldHaltUpgradeIfUpgradeConfigurationVetoesTheProcess()
    {
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
            newUpgrader( vetoingUpgradeConfiguration, new StoreMigrator( new SilentMigrationProgressMonitor(),
                    fileSystem ) )
                    .migrateIfNeeded( dbDirectory );
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

        try
        {
            newUpgrader( ALLOW_UPGRADE, new StoreMigrator( new SilentMigrationProgressMonitor(), fileSystem ) )
                    .migrateIfNeeded( dbDirectory );
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
        try
        {
            newUpgrader( ALLOW_UPGRADE, new StoreMigrator( new SilentMigrationProgressMonitor(), fileSystem ) )
                    .migrateIfNeeded( dbDirectory );
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

        try
        {
            newUpgrader( ALLOW_UPGRADE, new StoreMigrator( new SilentMigrationProgressMonitor(), fileSystem ) )
                    .migrateIfNeeded( dbDirectory );
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
        StoreUpgrader upgrader = new StoreUpgrader( ALLOW_UPGRADE, fileSystem, StoreUpgrader.NO_MONITOR );
        String failureMessage = "Just failing";
        upgrader.addParticipant( participant( "p1", "one", "two" ) );
        upgrader.addParticipant( participantThatWillFailWhenMoving( failureMessage ) );

        // WHEN
        try
        {
            upgrader.migrateIfNeeded( dbDirectory );
        }
        catch ( UnableToUpgradeException e )
        {   // THEN
            assertTrue( e.getCause() instanceof IOException );
            assertEquals( failureMessage, e.getCause().getMessage() );
        }

        // AND WHEN
        Monitor monitor = Mockito.mock( Monitor.class );
        upgrader = new StoreUpgrader( ALLOW_UPGRADE, fileSystem, monitor );
        upgrader.addParticipant( participant( "p1", "one", "two" ) );
        StoreMigrationParticipant observingParticipant = Mockito.mock( StoreMigrationParticipant.class );
        Mockito.when( observingParticipant.needsMigration(
                Matchers.any( FileSystemAbstraction.class ), Matchers.any( File.class ) ) ).thenReturn( true );
        upgrader.addParticipant( observingParticipant );
        upgrader.migrateIfNeeded( dbDirectory );

        // THEN
        Mockito.verify( observingParticipant, Mockito.times( 0 ) ).migrate( Matchers.any( FileSystemAbstraction.class ),
                Matchers.any( File.class ), Matchers.any( File.class ), Matchers.any( DependencyResolver.class ) );
        Mockito.verify( observingParticipant, Mockito.times( 1 ) ).moveMigratedFiles( Matchers.eq( fileSystem ),
                Matchers.any( File.class ), Matchers.any( File.class ), Matchers.any( File.class ) );
        Mockito.verify( observingParticipant, Mockito.times( 1 ) ).cleanup( Matchers.eq( fileSystem ), Matchers.any(
                File.class ) );
        Mockito.verify( monitor ).migrationCompleted();
    }

    private StoreMigrationParticipant participantThatWillFailWhenMoving( final String failureMessage )
    {
        return new StoreMigrationParticipant.Adapter()
        {
            @Override
            public boolean needsMigration( FileSystemAbstraction fileSystem, File storeDir ) throws IOException
            {
                return true;
            }

            @Override
            public void moveMigratedFiles( FileSystemAbstraction fileSystem, File migrationDir, File storeDir,
                                           File leftOversDir )
                    throws IOException
            {
                throw new IOException( failureMessage );
            }

            @Override
            public void migrate( FileSystemAbstraction fileSystem, File storeDir, File migrationDir,
                                 DependencyResolver dependencies ) throws IOException, UnsatisfiedDependencyException
            {   // Do nothing in particular
            }
        };
    }

    private StoreMigrationParticipant participant( final String directory, final String... files )
    {
        return new StoreMigrationParticipant.Adapter()
        {
            @Override
            public boolean needsMigration( FileSystemAbstraction fileSystem, File storeDir ) throws IOException
            {
                return true;
            }

            @Override
            public void migrate( FileSystemAbstraction fileSystem, File storeDir, File migrationDir,
                                 DependencyResolver dependencies ) throws IOException, UnsatisfiedDependencyException
            {
                File dir = new File( migrationDir, directory );
                fileSystem.mkdirs( dir );
                for ( String file : files )
                {
                    writeFile( fileSystem, new File( dir, file ), file );
                }
            }

            @Override
            public void moveMigratedFiles( FileSystemAbstraction fileSystem, File migrationDir, File storeDir,
                                           File leftOversDir ) throws IOException
            {
                for ( File file : fileSystem.listFiles( new File( migrationDir, directory ) ) )
                {
                    fileSystem.moveToDirectory( file, storeDir );
                }
            }
        };
    }

    private StoreUpgrader newUpgrader( UpgradeConfiguration config, StoreMigrator migrator )
    {
        StoreUpgrader upgrader = new StoreUpgrader( config, fileSystem, StoreUpgrader.NO_MONITOR );
        upgrader.addParticipant( migrator );
        return upgrader;
    }

    private void writeFile( FileSystemAbstraction fileSystem, File file, String contents ) throws IOException
    {
        try ( Writer writer = fileSystem.openAsWriter( file, "UTF-8", false ) )
        {
            writer.write( contents );
        }
    }
}
