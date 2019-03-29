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
package org.neo4j.io.fs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.FileWriter;

import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class FileSystemUtilsTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldCheckNonExistingDirectory()
    {
        File nonExistingDir = new File( "nonExistingDir" );

        assertTrue( FileSystemUtils.isEmptyOrNonExistingDirectory( fs, nonExistingDir ) );
    }

    @Test
    void shouldCheckExistingEmptyDirectory()
    {
        File existingEmptyDir = testDirectory.directory( "existingEmptyDir" );

        assertTrue( FileSystemUtils.isEmptyOrNonExistingDirectory( fs, existingEmptyDir ) );
    }

    @Test
    void shouldCheckExistingNonEmptyDirectory() throws Exception
    {
        File existingEmptyDir = testDirectory.directory( "existingEmptyDir" );
        fs.write( new File( existingEmptyDir, "someFile" ) ).close();

        assertFalse( FileSystemUtils.isEmptyOrNonExistingDirectory( fs, existingEmptyDir ) );
    }

    @Test
    void shouldCheckExistingFile()
    {
        File existingFile = testDirectory.createFile( "existingFile" );

        assertFalse( FileSystemUtils.isEmptyOrNonExistingDirectory( fs, existingFile ) );
    }

    @Test
    void shouldCheckSizeOfFile() throws Exception
    {
        File file = testDirectory.createFile( "a" );

        try ( FileWriter fileWriter = new FileWriter( file ) )
        {
            fileWriter.append( 'a' );
        }

        assertThat( FileSystemUtils.size( fs, file ), is( 1L ) );
    }

    @Test
    void shouldCheckSizeOfDirectory() throws Exception
    {
        File dir = testDirectory.directory( "dir" );
        File file1 = new File( dir, "file1" );
        File file2 = new File( dir, "file2" );

        try ( FileWriter fileWriter = new FileWriter( file1 ) )
        {
            fileWriter.append( 'a' ).append( 'b' );
        }
        try ( FileWriter fileWriter = new FileWriter( file2 ) )
        {
            fileWriter.append( 'a' );
        }

        assertThat( FileSystemUtils.size( fs, dir ), is( 3L ) );
    }
}
