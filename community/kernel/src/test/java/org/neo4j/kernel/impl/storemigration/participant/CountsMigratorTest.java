/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.storemigration.participant;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.util.monitoring.SilentProgressReporter;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.store.StoreFile.COUNTS_STORE_LEFT;
import static org.neo4j.kernel.impl.store.StoreFile.COUNTS_STORE_RIGHT;
import static org.neo4j.kernel.impl.storemigration.StoreFileType.STORE;

public class CountsMigratorTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory( fs );

    @Test
    public void shouldNotAccidentallyDeleteStoreFilesIfNoMigrationWasRequired() throws IOException
    {
        // given
        CountsMigrator migrator = new CountsMigrator( fs, null, Config.defaults() );
        File storeDir = directory.graphDbDir();
        File countsStoreFileA = new File( storeDir, COUNTS_STORE_LEFT.fileName( STORE ) );
        File countsStoreFileB = new File( storeDir, COUNTS_STORE_RIGHT.fileName( STORE ) );
        fs.create( countsStoreFileA );
        fs.create( countsStoreFileB );
        File migrationDir = new File( storeDir, "migration" );
        fs.mkdirs( migrationDir );
        String versionToMigrateFrom = StoreVersion.STANDARD_V3_2.versionString();
        String versionToMigrateTo = StoreVersion.STANDARD_V3_4.versionString();
        migrator.migrate( storeDir, migrationDir, SilentProgressReporter.INSTANCE, versionToMigrateFrom, versionToMigrateTo );
        assertEquals( "Invalid test assumption: There should not have been migration for those versions", 0,
                fs.listFiles( migrationDir ).length );

        // when
        migrator.moveMigratedFiles( migrationDir, storeDir, versionToMigrateFrom, versionToMigrateTo );

        // then
        assertTrue( fs.fileExists( countsStoreFileA ) );
        assertTrue( fs.fileExists( countsStoreFileB ) );
    }
}
