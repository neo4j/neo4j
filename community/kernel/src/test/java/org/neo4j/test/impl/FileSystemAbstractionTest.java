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
package org.neo4j.test.impl;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.test.TargetDirectory;

@RunWith(Parameterized.class)
public class FileSystemAbstractionTest
{
    private final FileSystemAbstraction fileSystem;

    @Parameterized.Parameters
    public static List<Object[]> fileSystemAbstractions()
    {
        return asList( new Object[]{new DefaultFileSystemAbstraction()},
                new Object[]{new EphemeralFileSystemAbstraction()} );
    }

    public FileSystemAbstractionTest( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
        testDirectory = TargetDirectory.testDirForTest( fileSystem, getClass() );
    }

    @Rule
    public TargetDirectory.TestDirectory testDirectory;

    @Test
    public void shouldCreateFileThenReportThatItExists() throws Exception
    {
        // given
        File fileToCreate = new File( testDirectory.directory(), "created" );

        // when
        FileChannel channel = fileSystem.create( fileToCreate );

        // then
        assertTrue( fileSystem.fileExists( fileToCreate ) );
        
        channel.close();
    }

    @Test
    public void shouldWriteBytesThenReadThemBack() throws Exception
    {
        // given
        byte[] someBytes = "some bytes".getBytes();
        File fileOfBytes = new File( testDirectory.directory(), "bytes" );
        OutputStream outputStream = fileSystem.openAsOutputStream( fileOfBytes, false );

        // when
        outputStream.write( someBytes );
        outputStream.close();

        // then
        InputStream inputStream = fileSystem.openAsInputStream( fileOfBytes );
        byte[] readBytes = new byte[someBytes.length];
        assertEquals( someBytes.length, inputStream.read( readBytes ) );
        assertArrayEquals( someBytes, readBytes );
        inputStream.close();
    }

    @Test
    public void shouldWriteCharactersThenReadThemBack() throws Exception
    {
        // given
        String string = "some bytes";
        File fileOfBytes = new File( testDirectory.directory(), "bytes" );
        Writer writer = fileSystem.openAsWriter( fileOfBytes, "utf-8", false );

        // when
        writer.write( string );
        writer.close();

        // then
        Reader reader = fileSystem.openAsReader( fileOfBytes, "utf-8" );
        char[] chars = new char[string.length()];
        assertEquals( string.length(), reader.read( chars ) );
        assertArrayEquals( string.toCharArray(), chars );
        reader.close();
    }

    @Test
    public void shouldRefuseToCreateAFileInADirectoryThatDoesNotExist() throws Exception
    {
        // given
        File imaginaryDirectory = new File( testDirectory.directory(), "imaginary_directory" );
        assertFalse( fileSystem.fileExists( imaginaryDirectory ) );

        try
        {
            // when
            fileSystem.create( new File( imaginaryDirectory, "will_not_be_created" ) );

            throw new IllegalStateException( "Should have thrown exception" );
        }
        catch ( FileNotFoundException e )
        {
            // then
            // expected
        }
    }

    @Test
    public void shouldCreateADirectoryThenAllowFilesToBeCreatedWithinIt() throws Exception
    {
        // given
        File newDirectory = new File( testDirectory.directory(), "new_directory" );
        assertFalse( fileSystem.fileExists( newDirectory ) );

        // when
        fileSystem.mkdir( newDirectory );

        // then
        fileSystem.create( new File( newDirectory, "file in new directory" ) ).close();
    }

    @Test
    public void makeDirectoryShouldReturnFalseWhenDirectoryAlreadyExists() throws Exception
    {
        // given
        File newDirectory = new File( testDirectory.directory(), "new_directory" );

        // when
        assertTrue( fileSystem.mkdir( newDirectory ) );

        // then
        assertFalse( fileSystem.mkdir( newDirectory ) );
    }

    @Test
    public void shouldMakeADeepTreeOfDirectories() throws Exception
    {
        // given
        File deepDirectory = new File( testDirectory.directory(), "a/b/c/d/e/f" );

        // when
        fileSystem.mkdirs( deepDirectory );

        // then
        assertTrue( fileSystem.isDirectory( new File( testDirectory.directory(), "a" ) ) );
        assertTrue( fileSystem.isDirectory( new File( testDirectory.directory(), "a/b" ) ) );
        assertTrue( fileSystem.isDirectory( new File( testDirectory.directory(), "a/b/c" ) ) );
        assertTrue( fileSystem.isDirectory( new File( testDirectory.directory(), "a/b/c/d" ) ) );
        assertTrue( fileSystem.isDirectory( new File( testDirectory.directory(), "a/b/c/d/e" ) ) );
        assertTrue( fileSystem.isDirectory( new File( testDirectory.directory(), "a/b/c/d/e/f" ) ) );
    }
}
