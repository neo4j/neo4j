/**
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.alwaysAllowed;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.changeVersionNumber;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.countLogicalLogs;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.containsAnyStoreFiles;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.defaultConfig;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.isolatedMigrationDirectoryOf;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.verifyFilesHaveSameContent;

public class StoreUpgraderTestIT
{
    @Test
    public void shouldUpgradeAnOldFormatStore() throws IOException
    {
        assertTrue( allStoreFilesHaveVersion( fileSystem, dbDirectory, LegacyStore.LEGACY_VERSION ) );

        StoreUpgrader upgrader = newUpgrader( alwaysAllowed(),
                new StoreMigrator( new SilentMigrationProgressMonitor() ),
                new DatabaseFiles( fileSystem ) );
        upgrader.attemptUpgrade( new File( dbDirectory, NeoStore.DEFAULT_NAME ) );

        assertTrue( allStoreFilesHaveVersion( fileSystem, dbDirectory, ALL_STORES_VERSION ) );

        assertThat( countLogicalLogs( fileSystem, dbDirectory ), is( 1 ) );
        
        assertFalse( containsAnyStoreFiles( fileSystem, isolatedMigrationDirectoryOf( dbDirectory ) ) );
    }

    @Test
    public void shouldLeaveACopyOfOriginalStoreFilesInBackupDirectory() throws IOException
    {
        StoreUpgrader upgrader = newUpgrader( alwaysAllowed(),
                new StoreMigrator( new SilentMigrationProgressMonitor() ), new DatabaseFiles( fileSystem ) );
        upgrader.attemptUpgrade( new File( dbDirectory, NeoStore.DEFAULT_NAME ) );

        File backupDirectory = new File( dbDirectory, "upgrade_backup" );

        verifyFilesHaveSameContent( fileSystem, MigrationTestUtils.findOldFormatStoreDirectory(), backupDirectory );

        // .v0, .v1 and .active
        assertThat( countLogicalLogs( fileSystem, backupDirectory ), is( 3 ) );
    }

    @Test
    public void shouldBackupOriginalStoreEvenIfMessagesLogIsMissing() throws IOException
    {
        // given
        fileSystem.deleteFile( new File( dbDirectory, StringLogger.DEFAULT_NAME ) );

        // when
        newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ), new DatabaseFiles( fileSystem ) )
                .attemptUpgrade( new File( dbDirectory, NeoStore.DEFAULT_NAME ) );

        // then
        File backupDirectory = new File( dbDirectory, "upgrade_backup" );
        assertFalse( fileSystem.fileExists( new File( dbDirectory, StringLogger.DEFAULT_NAME ) ) );
        assertFalse( fileSystem.fileExists( new File( backupDirectory, StringLogger.DEFAULT_NAME ) ) );
    }

    @Test
    public void shouldHaltUpgradeIfUpgradeConfigurationVetoesTheProcess() throws IOException
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
            newUpgrader( vetoingUpgradeConfiguration, new StoreMigrator( new SilentMigrationProgressMonitor() ),
                    new DatabaseFiles( fileSystem ) ).attemptUpgrade( new File( dbDirectory, NeoStore.DEFAULT_NAME ) );
            fail( "Should throw exception" );
        }
        catch ( UpgradeNotAllowedByConfigurationException e )
        {
            // expected
        }
    }

    @Test
    public void shouldLeaveAllFilesUntouchedIfWrongVersionNumberFound() throws IOException
    {
        File comparisonDirectory = new File( "target/" + StoreUpgraderTestIT.class.getSimpleName()
                + "shouldLeaveAllFilesUntouchedIfWrongVersionNumberFound-comparison" );

        changeVersionNumber( fileSystem, new File( dbDirectory, "neostore.nodestore.db" ), "v0.9.5" );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );

        try
        {
            newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ),
                    new DatabaseFiles( fileSystem ) ).attemptUpgrade( new File( dbDirectory, NeoStore.DEFAULT_NAME ) );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UnexpectedUpgradingStoreVersionException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fileSystem, comparisonDirectory, dbDirectory );
    }

    @Test
    public void shouldRefuseToUpgradeIfAnyOfTheStoresWeNotShutDownCleanly() throws IOException
    {
        File comparisonDirectory = new File( "target/" + StoreUpgraderTestIT.class.getSimpleName()
                + "shouldRefuseToUpgradeIfAnyOfTheStoresWeNotShutDownCleanly-comparison" );

        truncateFile( fileSystem, new File( dbDirectory, "neostore.propertystore.db.index.keys" ),
                "StringPropertyStore v0.9.9" );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );
        try
        {
            newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ),
                    new DatabaseFiles( fileSystem ) ).attemptUpgrade( new File( dbDirectory, NeoStore.DEFAULT_NAME ) );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UpgradingStoreVersionNotFoundException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fileSystem, comparisonDirectory, dbDirectory );
    }

    @Test
    public void shouldRefuseToUpgradeIfAllOfTheStoresWereNotShutDownCleanly() throws IOException
    {
        File comparisonDirectory = new File( "target/" + StoreUpgraderTestIT.class.getSimpleName()
                + "shouldRefuseToUpgradeIfAllOfTheStoresWeNotShutDownCleanly-comparison" );

        truncateAllFiles( fileSystem, dbDirectory );
        fileSystem.deleteRecursively( comparisonDirectory );
        fileSystem.copyRecursively( dbDirectory, comparisonDirectory );

        try
        {
            newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ),
                    new DatabaseFiles( fileSystem ) ).attemptUpgrade( new File( dbDirectory, NeoStore.DEFAULT_NAME ) );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UpgradingStoreVersionNotFoundException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fileSystem, comparisonDirectory, dbDirectory );
    }

    public static void truncateAllFiles( FileSystemAbstraction fileSystem, File workingDirectory ) throws IOException
    {
        for ( StoreFile storeFile : StoreFile.legacyStoreFiles() )
        {
            truncateFile( fileSystem, new File( workingDirectory, storeFile.storeFileName() ), storeFile.legacyVersion() );
        }
    }

    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final File dbDirectory = new File( "dir" );
    private EphemeralFileSystemAbstraction fileSystem;

    private StoreUpgrader newUpgrader( UpgradeConfiguration config, StoreMigrator migrator, DatabaseFiles files )
    {
        return new StoreUpgrader( defaultConfig(), config, new UpgradableDatabase( new StoreVersionCheck( fs.get() ) ), migrator,
                files, new DefaultIdGeneratorFactory(), fs.get() );
    }

    @Before
    public void before() throws Exception
    {
        fileSystem = fs.get();
        prepareSampleLegacyDatabase( fileSystem, dbDirectory );
    }
}
