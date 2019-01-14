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
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.util.monitoring.SilentProgressReporter;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CountsMigratorTest
{
    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fs );
    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fs ).around( directory );

    @Test
    public void shouldNotAccidentallyDeleteStoreFilesIfNoMigrationWasRequired() throws IOException
    {
        // given
        CountsMigrator migrator = new CountsMigrator( fs, null, Config.defaults() );
        DatabaseLayout sourceLayout  = directory.databaseLayout();
        File countsStoreFileA = sourceLayout.countStoreA();
        File countsStoreFileB = sourceLayout.countStoreB();
        fs.create( countsStoreFileA );
        fs.create( countsStoreFileB );
        DatabaseLayout migrationLayout = directory.databaseLayout( "migration" );
        String versionToMigrateFrom = StoreVersion.STANDARD_V3_2.versionString();
        String versionToMigrateTo = StoreVersion.STANDARD_V3_4.versionString();
        migrator.migrate( sourceLayout, migrationLayout, SilentProgressReporter.INSTANCE, versionToMigrateFrom, versionToMigrateTo );
        assertEquals( "Invalid test assumption: There should not have been migration for those versions", 0,
                migrationLayout.listDatabaseFiles( ( dir, name ) -> true ).length );

        // when
        migrator.moveMigratedFiles( migrationLayout, sourceLayout, versionToMigrateFrom, versionToMigrateTo );

        // then
        assertTrue( fs.fileExists( countsStoreFileA ) );
        assertTrue( fs.fileExists( countsStoreFileB ) );
    }
}
