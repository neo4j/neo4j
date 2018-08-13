/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.io.layout;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( TestDirectoryExtension.class )
class DatabaseLayoutTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void databaseLayoutForAbsoluteFile()
    {
        File databaseDir = testDirectory.databaseDir();
        DatabaseLayout databaseLayout = DatabaseLayout.of( databaseDir );
        assertEquals( databaseLayout.databaseDirectory(), databaseDir );
    }

    @Test
    void databaseLayoutResolvesLinks() throws IOException
    {
        Path basePath = testDirectory.directory().toPath();
        File databaseDir = testDirectory.databaseDir("notAbsolute");
        Path linkPath = basePath.resolve( "link" );
        Path symbolicLink = Files.createSymbolicLink( linkPath, databaseDir.toPath() );
        DatabaseLayout databaseLayout = DatabaseLayout.of( symbolicLink.toFile() );
        assertEquals( databaseLayout.databaseDirectory(), databaseDir );
    }

    @Test
    void databaseLayoutUseCanonicalRepresentation()
    {
        File storeDir = testDirectory.storeDir( "notCanonical" );
        Path basePath = testDirectory.databaseDir( storeDir ).toPath();
        Path notCanonicalPath = basePath.resolve( "../anotherDatabase" );
        DatabaseLayout databaseLayout = DatabaseLayout.of( notCanonicalPath.toFile() );
        File expectedDirectory = StoreLayout.of( storeDir ).databaseLayout( "anotherDatabase" ).databaseDirectory();
        assertEquals( expectedDirectory, databaseLayout.databaseDirectory() );
    }

    @Test
    void databaseLayoutForName()
    {
        String databaseName = "testDatabase";
        StoreLayout storeLayout = testDirectory.storeLayout();
        DatabaseLayout testDatabase = DatabaseLayout.of( storeLayout, databaseName );
        assertEquals( new File( storeLayout.storeDirectory(), databaseName ), testDatabase.databaseDirectory() );
    }

    @Test
    void databaseLayoutForFolderAndName()
    {
        String database = "database";
        DatabaseLayout databaseLayout = DatabaseLayout.of( testDirectory.storeDir(), database );
        assertEquals( testDirectory.databaseLayout( database ).databaseDirectory(), databaseLayout.databaseDirectory() );
    }
}
