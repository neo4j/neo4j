/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.alwaysAllowed;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.changeVersionNumber;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.defaultConfig;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.verifyFilesHaveSameContent;
import static org.neo4j.kernel.impl.util.FileUtils.copyRecursively;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Test;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.util.StringLogger;

public class StoreUpgraderTestIT
{
    @Test
    public void shouldUpgradeAnOldFormatStore() throws IOException
    {
        File workingDirectory = new File(
                "target/" + StoreUpgraderTestIT.class.getSimpleName()
                        + "shouldUpgradeAnOldFormatStore" );
        MigrationTestUtils.prepareSampleLegacyDatabase( fileSystem, workingDirectory );

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, "v0.9.9" ) );

        newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ) , new DatabaseFiles() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ) );

        assertTrue( allStoreFilesHaveVersion( fileSystem, workingDirectory, ALL_STORES_VERSION ) );
    }

    @Test
    public void shouldLeaveACopyOfOriginalStoreFilesInBackupDirectory() throws IOException
    {
        File workingDirectory = new File(
                "target/"
                        + StoreUpgraderTestIT.class.getSimpleName()
                        + "shouldLeaveACopyOfOriginalStoreFilesInBackupDirectory" );
        MigrationTestUtils.prepareSampleLegacyDatabase( fileSystem, workingDirectory );

        newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ) , new DatabaseFiles() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ) );

        verifyFilesHaveSameContent( fileSystem, MigrationTestUtils.findOldFormatStoreDirectory(), new File( workingDirectory, "upgrade_backup" ) );
    }

    @Test
    public void shouldHaltUpgradeIfUpgradeConfigurationVetoesTheProcess() throws IOException
    {
        File workingDirectory = new File(
                "target/"
                        + StoreUpgraderTestIT.class.getSimpleName()
                        + "shouldHaltUpgradeIfUpgradeConfigurationVetoesTheProcess" );
        MigrationTestUtils.prepareSampleLegacyDatabase( fileSystem, workingDirectory );

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
            newUpgrader( vetoingUpgradeConfiguration, new StoreMigrator( new SilentMigrationProgressMonitor() ), new DatabaseFiles() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ) );
            fail( "Should throw exception" );
        } catch ( UpgradeNotAllowedByConfigurationException e )
        {
            // expected
        }
    }

    @Test
    public void shouldLeaveAllFilesUntouchedIfWrongVersionNumberFound() throws IOException
    {
        File workingDirectory = new File(
                "target/"
                        + StoreUpgraderTestIT.class.getSimpleName()
                        + "shouldLeaveAllFilesUntouchedIfWrongVersionNumberFound" );
        File comparisonDirectory = new File(
                "target/"
                        + StoreUpgraderTestIT.class.getSimpleName()
                        + "shouldLeaveAllFilesUntouchedIfWrongVersionNumberFound-comparison" );
        MigrationTestUtils.prepareSampleLegacyDatabase( fileSystem, workingDirectory );

        changeVersionNumber( fileSystem, new File( workingDirectory, "neostore.nodestore.db" ), "v0.9.5" );
        deleteRecursively( comparisonDirectory );
        copyRecursively( workingDirectory, comparisonDirectory );

        try
        {
            newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ), new DatabaseFiles() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ) );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fileSystem, comparisonDirectory, workingDirectory );
    }

    @Test
    public void shouldRefuseToUpgradeIfAnyOfTheStoresWeNotShutDownCleanly() throws IOException
    {
        File workingDirectory = new File(
                "target/"
                        + StoreUpgraderTestIT.class.getSimpleName()
                        + "shouldRefuseToUpgradeIfAnyOfTheStoresWeNotShutDownCleanly" );
        File comparisonDirectory = new File(
                "target/"
                        + StoreUpgraderTestIT.class.getSimpleName()
                        + "shouldRefuseToUpgradeIfAnyOfTheStoresWeNotShutDownCleanly-comparison" );
        MigrationTestUtils.prepareSampleLegacyDatabase( fileSystem, workingDirectory );

        truncateFile( fileSystem, new File( workingDirectory,
                "neostore.propertystore.db.index.keys" ),
                "StringPropertyStore v0.9.9" );
        deleteRecursively( comparisonDirectory );
        copyRecursively( workingDirectory, comparisonDirectory );

        try
        {
            newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ), new DatabaseFiles() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ) );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fileSystem, comparisonDirectory, workingDirectory );
    }

    @Test
    public void shouldRefuseToUpgradeIfAllOfTheStoresWeNotShutDownCleanly()
            throws IOException
    {
        File workingDirectory = new File(
                "target/"
                        + StoreUpgraderTestIT.class.getSimpleName()
                        + "shouldRefuseToUpgradeIfAllOfTheStoresWeNotShutDownCleanly" );
        File comparisonDirectory = new File(
                "target/" + StoreUpgraderTestIT.class.getSimpleName()
                        + "shouldRefuseToUpgradeIfAllOfTheStoresWeNotShutDownCleanly-comparison" );
        MigrationTestUtils.prepareSampleLegacyDatabase( fileSystem, workingDirectory );

        truncateAllFiles( fileSystem, workingDirectory );
        deleteRecursively( comparisonDirectory );
        copyRecursively( workingDirectory, comparisonDirectory );

        try
        {
            newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ),
                    new DatabaseFiles() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ) );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fileSystem, comparisonDirectory, workingDirectory );
    }

    public static void truncateAllFiles( FileSystemAbstraction fileSystem, File workingDirectory )
            throws IOException
    {
        for ( Map.Entry<String, String> legacyFile : UpgradableDatabase.fileNamesToExpectedVersions.entrySet() )
        {
            truncateFile( fileSystem, new File( workingDirectory, legacyFile.getKey()),
                    legacyFile.getValue() );
        }
    }

    private final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
    
    private StoreUpgrader newUpgrader( UpgradeConfiguration config, StoreMigrator migrator, DatabaseFiles files )
    {
        return new StoreUpgrader( defaultConfig(), StringLogger.DEV_NULL, config, new UpgradableDatabase(), migrator,
                files, new DefaultIdGeneratorFactory(), fileSystem );        
    }
}
