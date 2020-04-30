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
package org.neo4j.io.fs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;

import org.neo4j.test.extension.DisabledForRoot;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.internal.helpers.Numbers.isPowerOfTwo;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.UNABLE_TO_CREATE_DIRECTORY_FORMAT;
import static org.neo4j.io.fs.FileSystemAbstraction.INVALID_FILE_DESCRIPTOR;

public class DefaultFileSystemAbstractionTest extends FileSystemAbstractionTest
{
    @Override
    protected FileSystemAbstraction buildFileSystemAbstraction()
    {
        return new DefaultFileSystemAbstraction();
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void retrieveFileDescriptor() throws IOException
    {
        File testFile = testDirectory.file( "testFile" );
        try ( StoreChannel storeChannel = fsa.write( testFile ) )
        {
            int fileDescriptor = fsa.getFileDescriptor( storeChannel );
            assertThat( fileDescriptor ).isGreaterThan( 0 );
        }
    }

    @Test
    @EnabledOnOs( OS.WINDOWS )
    void retrieveWindowsFileDescriptor() throws IOException
    {
        File testFile = testDirectory.file( "testFile" );
        try ( StoreChannel storeChannel = fsa.write( testFile ) )
        {
            int fileDescriptor = fsa.getFileDescriptor( storeChannel );
            assertThat( fileDescriptor ).isEqualTo( INVALID_FILE_DESCRIPTOR );
        }
    }

    @Test
    void retrieveFileDescriptorOnClosedChannel() throws IOException
    {
        File testFile = testDirectory.file( "testFile" );
        StoreChannel escapedChannel = null;
        try ( StoreChannel storeChannel = fsa.write( testFile ) )
        {
            escapedChannel = storeChannel;
        }
        int fileDescriptor = fsa.getFileDescriptor( escapedChannel );
        assertThat( fileDescriptor ).isEqualTo( INVALID_FILE_DESCRIPTOR );
    }

    @Test
    void retrieveBlockSize() throws IOException
    {
        var testFile = testDirectory.createFile( "testBlock" );
        long blockSize = fsa.getBlockSize( testFile );
        assertTrue( isPowerOfTwo( blockSize ), "Observed block size: " + blockSize );
        assertThat( blockSize ).isGreaterThanOrEqualTo( 512L );
    }

    @Test
    // Windows doesn't seem to be able to set a directory read only without complex ACLs
    @DisabledOnOs( OS.WINDOWS )
    @DisabledForRoot
    void shouldFailGracefullyWhenPathCannotBeCreated() throws Exception
    {
        Files.createDirectories( path.toPath() );
        assertTrue( fsa.fileExists( path ) );
        assumeTrue( path.setWritable( false ) );
        path = new File( path, "some_file" );

        IOException exception = assertThrows( IOException.class, () -> fsa.mkdirs( path ) );
        assertFalse( fsa.isDirectory( path ) );
        String expectedMessage = format( UNABLE_TO_CREATE_DIRECTORY_FORMAT, path );
        assertThat( exception.getMessage() ).isEqualTo( expectedMessage );
        Throwable cause = exception.getCause();
        assertThat( cause ).isInstanceOf( AccessDeniedException.class );
    }
}
