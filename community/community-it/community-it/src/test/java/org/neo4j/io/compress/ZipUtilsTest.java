/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.io.compress;

import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipInputStream;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
class ZipUtilsTest
{

    @Inject
    TestDirectory testDirectory;
    @Inject
    DefaultFileSystemAbstraction fileSystem;

    @Test
    void doNotCreateZipArchiveForNonExistentSource() throws IOException
    {
        Path archiveFile = testDirectory.filePath( "archive.zip" );
        ZipUtils.zip( fileSystem, testDirectory.filePath( "doesNotExist" ), archiveFile );
        assertFalse( fileSystem.fileExists( archiveFile.toFile() ) );
    }

    @Test
    void doNotCreateZipArchiveForEmptyDirectory() throws IOException
    {
        Path archiveFile = testDirectory.filePath( "archive.zip" );
        Path emptyDirectory = testDirectory.directoryPath( "emptyDirectory" );
        ZipUtils.zip( fileSystem, emptyDirectory, archiveFile );
        assertFalse( fileSystem.fileExists( archiveFile.toFile() ) );
    }

    @Test
    void archiveDirectory() throws IOException
    {
        Path archiveFile = testDirectory.filePath( "directoryArchive.zip" );
        Path directory = testDirectory.directoryPath( "directory" );
        fileSystem.write( directory.resolve( "a" ).toFile() ).close();
        fileSystem.write( directory.resolve( "b" ).toFile() ).close();
        ZipUtils.zip( fileSystem, directory, archiveFile );

        assertTrue( fileSystem.fileExists( archiveFile.toFile() ) );
        assertEquals( 2, countArchiveEntries( archiveFile ) );
    }

    @Test
    void archiveDirectoryWithSubdirectories() throws IOException
    {
        Path archiveFile = testDirectory.filePath( "directoryWithSubdirectoriesArchive.zip" );
        Path directoryArchive = testDirectory.directoryPath( "directoryWithSubdirs" );
        Path subdir1 = directoryArchive.resolve( "subdir1" );
        Path subdir2 = directoryArchive.resolve( "subdir" );
        fileSystem.mkdir( subdir1.toFile() );
        fileSystem.mkdir( subdir2.toFile() );
        fileSystem.write( directoryArchive.resolve( "a" ).toFile() ).close();
        fileSystem.write( directoryArchive.resolve( "b" ).toFile() ).close();
        fileSystem.write( subdir1.resolve( "c" ).toFile() ).close();
        fileSystem.write( subdir2.resolve( "d" ).toFile() ).close();

        ZipUtils.zip( fileSystem, directoryArchive, archiveFile );

        assertTrue( fileSystem.fileExists( archiveFile.toFile() ) );
        assertEquals( 6, countArchiveEntries( archiveFile ) );
    }

    @Test
    void archiveFile() throws IOException
    {
        Path archiveFile = testDirectory.filePath( "fileArchive.zip" );
        Path aFile = testDirectory.filePath( "a" );
        fileSystem.write( aFile.toFile() ).close();
        ZipUtils.zip( fileSystem, aFile, archiveFile );

        assertTrue( fileSystem.fileExists( archiveFile.toFile() ) );
        assertEquals( 1, countArchiveEntries( archiveFile ) );
    }

    @Test
    public void supportSpacesInDestinationPath() throws IOException
    {
        Path archiveFile = testDirectory.filePath( "file archive.zip" );
        Path aFile = testDirectory.filePath( "a" );
        fileSystem.write( aFile.toFile() ).close();
        ZipUtils.zip( fileSystem, aFile, archiveFile );
    }

    private int countArchiveEntries( Path archiveFile ) throws IOException
    {
        try ( ZipInputStream zipInputStream = new ZipInputStream( new BufferedInputStream( Files.newInputStream( archiveFile ) ) ) )
        {
            int entries = 0;
            while ( zipInputStream.getNextEntry() != null )
            {
                entries++;
            }
            return entries;
        }
    }
}
