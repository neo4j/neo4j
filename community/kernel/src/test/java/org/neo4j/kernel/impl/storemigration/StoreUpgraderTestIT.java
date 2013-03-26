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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.alwaysAllowed;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.changeVersionNumber;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.defaultConfig;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.prepareSampleLegacyDatabase;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.truncateFile;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.verifyFilesHaveSameContent;

public class StoreUpgraderTestIT
{
    @Test
    public void shouldUpgradeAnOldFormatStore() throws IOException
    {
        assertTrue( allStoreFilesHaveVersion( fs.get(), workingDirectory, "v0.9.9" ) );

        newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ),
                new DatabaseFiles( fs.get() ) ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ) );

        assertTrue( allStoreFilesHaveVersion( fs.get(), workingDirectory, ALL_STORES_VERSION ) );
    }

    @Test
    public void shouldLeaveACopyOfOriginalStoreFilesInBackupDirectory() throws IOException
    {
        newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ), new DatabaseFiles( fs.get() ) )
                .attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ) );

        verifyFilesHaveSameContent( fs.get(), MigrationTestUtils.findOldFormatStoreDirectory(), new File(
                workingDirectory, "upgrade_backup" ) );
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
                    new DatabaseFiles( fs.get() ) ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ) );
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

        changeVersionNumber( fs.get(), new File( workingDirectory, "neostore.nodestore.db" ), "v0.9.5" );
        fs.get().deleteRecursively( comparisonDirectory );
        fs.get().copyRecursively( workingDirectory, comparisonDirectory );

        try
        {
            newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ),
                    new DatabaseFiles( fs.get() ) ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ) );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fs.get(), comparisonDirectory, workingDirectory );
    }

    @Test
    public void shouldRefuseToUpgradeIfAnyOfTheStoresWeNotShutDownCleanly() throws IOException
    {
        File comparisonDirectory = new File( "target/" + StoreUpgraderTestIT.class.getSimpleName()
                + "shouldRefuseToUpgradeIfAnyOfTheStoresWeNotShutDownCleanly-comparison" );

        truncateFile( fs.get(), new File( workingDirectory, "neostore.propertystore.db.index.keys" ),
                "StringPropertyStore v0.9.9" );
        fs.get().deleteRecursively( comparisonDirectory );
        fs.get().copyRecursively( workingDirectory, comparisonDirectory );

        try
        {
            newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ),
                    new DatabaseFiles( fs.get() ) ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ) );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fs.get(), comparisonDirectory, workingDirectory );
    }

    @Test
    public void shouldRefuseToUpgradeIfAllOfTheStoresWereNotShutDownCleanly() throws IOException
    {
        File comparisonDirectory = new File( "target/" + StoreUpgraderTestIT.class.getSimpleName()
                + "shouldRefuseToUpgradeIfAllOfTheStoresWeNotShutDownCleanly-comparison" );

        truncateAllFiles( fs.get(), workingDirectory );
        fs.get().deleteRecursively( comparisonDirectory );
        fs.get().copyRecursively( workingDirectory, comparisonDirectory );

        try
        {
            newUpgrader( alwaysAllowed(), new StoreMigrator( new SilentMigrationProgressMonitor() ),
                    new DatabaseFiles( fs.get() ) ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ) );
            fail( "Should throw exception" );
        }
        catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            // expected
        }

        verifyFilesHaveSameContent( fs.get(), comparisonDirectory, workingDirectory );
    }

    public static void truncateAllFiles( FileSystemAbstraction fileSystem, File workingDirectory ) throws IOException
    {
        for ( Map.Entry<String, String> legacyFile : UpgradableDatabase.fileNamesToExpectedVersions.entrySet() )
        {
            truncateFile( fileSystem, new File( workingDirectory, legacyFile.getKey() ), legacyFile.getValue() );
        }
    }

    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final File workingDirectory = new File( "dir" );

    private StoreUpgrader newUpgrader( UpgradeConfiguration config, StoreMigrator migrator, DatabaseFiles files )
    {
        return new StoreUpgrader( defaultConfig(), StringLogger.DEV_NULL, config, new UpgradableDatabase( fs.get() ), migrator,
                files, new DefaultIdGeneratorFactory(), fs.get() );
    }
    
    @Before
    public void before() throws Exception
    {
        fs.get().mkdirs( workingDirectory );
        prepareSampleLegacyDatabase( fs.get(), workingDirectory );
    }
}
