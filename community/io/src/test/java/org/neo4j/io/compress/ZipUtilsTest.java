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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.ZipInputStream;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class ZipUtilsTest
{

    @Inject
    TestDirectory testDirectory;
    @Inject
    DefaultFileSystemAbstraction fileSystem;

    @Test
    void doNotCreateZipArchiveForNonExistentSource() throws IOException
    {
        File archiveFile = testDirectory.file( "archive.zip" );
        ZipUtils.zip( fileSystem, testDirectory.file( "doesNotExist" ), archiveFile );
        assertFalse( fileSystem.fileExists( archiveFile ) );
    }

    @Test
    void doNotCreateZipArchiveForEmptyDirectory() throws IOException
    {
        File archiveFile = testDirectory.file( "archive.zip" );
        File emptyDirectory = testDirectory.directory( "emptyDirectory" );
        ZipUtils.zip( fileSystem, emptyDirectory, archiveFile );
        assertFalse( fileSystem.fileExists( archiveFile ) );
    }

    @Test
    void archiveDirectory() throws IOException
    {
        File archiveFile = testDirectory.file( "directoryArchive.zip" );
        File directory = testDirectory.directory( "directory" );
        fileSystem.create( new File( directory, "a" ) ).close();
        fileSystem.create( new File( directory, "b" ) ).close();
        ZipUtils.zip( fileSystem, directory, archiveFile );

        assertTrue( fileSystem.fileExists( archiveFile ) );
        assertEquals( 2, countArchiveEntries( archiveFile ) );
    }

    @Test
    void archiveDirectoryWithSubdirectories() throws IOException
    {
        File archiveFile = testDirectory.file( "directoryWithSubdirectoriesArchive.zip" );
        File directoryArchive = testDirectory.directory( "directoryWithSubdirs" );
        File subdir1 = new File( directoryArchive, "subdir1" );
        File subdir2 = new File( directoryArchive, "subdir" );
        fileSystem.mkdir( subdir1 );
        fileSystem.mkdir( subdir2 );
        fileSystem.create( new File( directoryArchive, "a" ) ).close();
        fileSystem.create( new File( directoryArchive, "b" ) ).close();
        fileSystem.create( new File( subdir1, "c" ) ).close();
        fileSystem.create( new File( subdir2, "d" ) ).close();

        ZipUtils.zip( fileSystem, directoryArchive, archiveFile );

        assertTrue( fileSystem.fileExists( archiveFile ) );
        assertEquals( 6, countArchiveEntries( archiveFile ) );
    }

    @Test
    void archiveFile() throws IOException
    {
        File archiveFile = testDirectory.file( "fileArchive.zip" );
        File aFile = testDirectory.file( "a" );
        fileSystem.create( aFile ).close();
        ZipUtils.zip( fileSystem, aFile, archiveFile );

        assertTrue( fileSystem.fileExists( archiveFile ) );
        assertEquals( 1, countArchiveEntries( archiveFile ) );
    }

    @Test
    public void supportSpacesInDestinationPath() throws IOException
    {
        File archiveFile = testDirectory.file( "file archive.zip" );
        File aFile = testDirectory.file( "a" );
        fileSystem.create( aFile ).close();
        ZipUtils.zip( fileSystem, aFile, archiveFile );
    }

    private int countArchiveEntries( File archiveFile ) throws IOException
    {
        try ( ZipInputStream zipInputStream = new ZipInputStream( new BufferedInputStream( new FileInputStream( archiveFile ) ) ) )
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
