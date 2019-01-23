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
class StoreLayoutTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void storeLayoutForAbsoluteFile()
    {
        File storeDir = testDirectory.storeDir();
        StoreLayout storeLayout = StoreLayout.of( storeDir );
        assertEquals( storeDir, storeLayout.storeDirectory() );
    }

    @Test
    void storeLayoutResolvesLinks() throws IOException
    {
        Path basePath = testDirectory.directory().toPath();
        File storeDir = testDirectory.storeDir("notAbsolute");
        Path linkPath = basePath.resolve( "link" );
        Path symbolicLink = Files.createSymbolicLink( linkPath, storeDir.toPath() );
        StoreLayout storeLayout = StoreLayout.of( symbolicLink.toFile() );
        assertEquals( storeDir, storeLayout.storeDirectory() );
    }

    @Test
    void storeLayoutUseCanonicalRepresentation()
    {
        Path basePath = testDirectory.storeDir("notCanonical").toPath();
        Path notCanonicalPath = basePath.resolve( "../anotherLocation" );
        StoreLayout storeLayout = StoreLayout.of( notCanonicalPath.toFile() );
        assertEquals( testDirectory.directory( "anotherLocation" ), storeLayout.storeDirectory() );
    }

    @Test
    void storeLockFileLocation()
    {
        StoreLayout storeLayout = testDirectory.storeLayout();
        File storeLockFile = storeLayout.storeLockFile();
        assertEquals( "store_lock", storeLockFile.getName() );
        assertEquals( storeLayout.storeDirectory(), storeLockFile.getParentFile() );
    }
}
