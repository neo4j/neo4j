/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.allStoreFilesHaveVersion;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.alwaysAllowed;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.defaultConfig;

import java.io.File;
import java.io.IOException;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;

public class StoreUpgraderInterruptionTest
{
    @Test
    public void shouldSucceedWithUpgradeAfterPreviousAttemptDiedDuringMigration() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderInterruptionTest.class.getSimpleName() );
        MigrationTestUtils.prepareSampleLegacyDatabase( workingDirectory );

        StoreMigrator failingStoreMigrator = new StoreMigrator( new SilentMigrationProgressMonitor() )
        {

            public void migrate( LegacyStore legacyStore, NeoStore neoStore ) throws IOException
            {
                super.migrate( legacyStore, neoStore );
                throw new RuntimeException( "This upgrade is failing" );
            }
        };

        assertTrue( allStoreFilesHaveVersion( workingDirectory, "v0.9.9" ) );

        try
        {
            new StoreUpgrader( defaultConfig(), alwaysAllowed(), new UpgradableDatabase(), failingStoreMigrator, new DatabaseFiles() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ).getPath() );
            fail( "Should throw exception" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( "This upgrade is failing", e.getMessage() );
        }

        assertTrue( allStoreFilesHaveVersion( workingDirectory, "v0.9.9" ) );

        new StoreUpgrader( defaultConfig(), alwaysAllowed(), new UpgradableDatabase(), new StoreMigrator( new SilentMigrationProgressMonitor() ), new DatabaseFiles() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ).getPath() );

        assertTrue( allStoreFilesHaveVersion( workingDirectory, ALL_STORES_VERSION ) );
    }

    @Test
    public void shouldFailOnSecondAttemptIfPreviousAttemptMadeABackupToAvoidDamagingBackup() throws IOException
    {
        File workingDirectory = new File( "target/" + StoreUpgraderInterruptionTest.class.getSimpleName() );
        MigrationTestUtils.prepareSampleLegacyDatabase( workingDirectory );

        DatabaseFiles failsOnBackup = new DatabaseFiles()
        {

            public void moveToBackupDirectory( File workingDirectory, File backupDirectory )
            {
                backupDirectory.mkdir();
                throw new RuntimeException( "Failing to backup working directory" );
            }
        };

        assertTrue( allStoreFilesHaveVersion( workingDirectory, "v0.9.9" ) );

        try
        {
            new StoreUpgrader( defaultConfig(), alwaysAllowed(), new UpgradableDatabase(), new StoreMigrator( new SilentMigrationProgressMonitor() ) , failsOnBackup ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ).getPath() );
            fail( "Should throw exception" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( "Failing to backup working directory", e.getMessage() );
        }

        try
        {
            new StoreUpgrader( defaultConfig(), alwaysAllowed(), new UpgradableDatabase(), new StoreMigrator( new SilentMigrationProgressMonitor() ) , new DatabaseFiles() ).attemptUpgrade( new File( workingDirectory, NeoStore.DEFAULT_NAME ).getPath() );
            fail( "Should throw exception" );
        }
        catch ( Exception e )
        {
            assertTrue( e.getMessage().startsWith( "Cannot proceed with upgrade because there is an existing upgrade backup in the way at " ) );
        }
    }

}
