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
package org.neo4j.internal.id;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Set;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class IdContainerTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;
    private File file;

    @BeforeEach
    void setUp()
    {
        file = testDirectory.file( "ids" );
    }

    @Test
    void shouldDeleteIfOpen()
    {
        // GIVEN
        createEmptyFile();
        IdContainer idContainer = new IdContainer( fs, file, 100, false );
        idContainer.init();

        // WHEN
        idContainer.delete();

        // THEN
        assertFalse( fs.fileExists( file ) );

        idContainer.close( 0 );
    }

    @Test
    void shouldDeleteIfClosed()
    {
        // GIVEN
        createEmptyFile();
        IdContainer idContainer = new IdContainer( fs, file, 100, false );
        idContainer.init();
        idContainer.close( 0 );

        // WHEN
        idContainer.delete();

        // THEN
        assertFalse( fs.fileExists( file ) );
    }

    @Test
    void shouldForceStickyMark() throws Exception
    {
        // GIVEN
        createEmptyFile();

        // WHEN opening the id generator, where the jvm crashes right after
        IdContainer idContainer = new IdContainer( fs, file, 100, false );
        idContainer.init();

        assertThrows( InvalidIdGeneratorException.class, () -> IdContainer.readHighId( fs, file ) );
        idContainer.close( 0 );
    }

    @Test
    void shouldTruncateTheFileIfOverwriting() throws Exception
    {
        // GIVEN
        IdContainer.createEmptyIdFile( fs, file, 30, false );
        IdContainer idContainer = new IdContainer( fs, file, 5, false );
        idContainer.init();
        for ( int i = 0; i < 17; i++ )
        {
            idContainer.freeId( i );
        }
        idContainer.close( 30 );
        assertThat( (int) fs.getFileSize( file ), greaterThan( IdContainer.HEADER_SIZE ) );

        // WHEN
        IdContainer.createEmptyIdFile( fs, file, 30, false );

        // THEN
        assertEquals( IdContainer.HEADER_SIZE, (int) fs.getFileSize( file ) );
        assertEquals( 30, IdContainer.readHighId( fs, file ) );
        idContainer = new IdContainer( fs, file, 5, false );
        idContainer.init();
        assertEquals( 30, idContainer.getInitialHighId() );

        idContainer.close( 30 );
    }

    @Test
    void shouldReturnFalseOnInitIfTheFileWasCreated()
    {
        // When
        // An IdContainer is created with no underlying file
        IdContainer idContainer = new IdContainer( fs, file, 100, false );

        // Then
        // Init should return false
        assertFalse( idContainer.init() );
        idContainer.close( 100 );
    }

    @Test
    void shouldReturnTrueOnInitIfAProperFileWasThere()
    {
        // Given
        // A properly created and closed id file
        IdContainer idContainer = new IdContainer( fs, file, 100, false );
        idContainer.init();
        idContainer.close( 100 );

        // When
        // An IdContainer is created over it
        idContainer = new IdContainer( fs, file, 100, false );

        // Then
        // init() should return true
        assertTrue( idContainer.init() );
        idContainer.close( 100 );
    }

    @Test
    void idContainerReadWriteBySingleByte() throws IOException
    {
        SingleByteFileSystemAbstraction fileSystem = new SingleByteFileSystemAbstraction();
        IdContainer idContainer = new IdContainer( fileSystem, file, 100, false );
        idContainer.init();
        idContainer.close( 100 );

        idContainer = new IdContainer( fileSystem, file, 100, false );
        idContainer.init();
        assertEquals( 100, idContainer.getInitialHighId() );
        fileSystem.close();
        idContainer.close( 100 );
    }

    private void createEmptyFile()
    {
        IdContainer.createEmptyIdFile( fs, file, 42, false );
    }

    private static class SingleByteFileSystemAbstraction extends DefaultFileSystemAbstraction
    {
        @Override
        public StoreFileChannel open( File fileName, Set<OpenOption> options ) throws IOException
        {
            return new SingleByteBufferChannel( super.open( fileName, options ) );
        }
    }

    private static class SingleByteBufferChannel extends StoreFileChannel
    {

        SingleByteBufferChannel( StoreFileChannel channel )
        {
            super( channel );
        }

        @Override
        public int write( ByteBuffer src ) throws IOException
        {
            byte b = src.get();
            ByteBuffer byteBuffer = ByteBuffer.wrap( new byte[]{b} );
            return super.write( byteBuffer );
        }

        @Override
        public int read( ByteBuffer dst ) throws IOException
        {
            ByteBuffer byteBuffer = ByteBuffer.allocate( 1 );
            int read = super.read( byteBuffer );
            if ( read > 0 )
            {
                byteBuffer.flip();
                dst.put( byteBuffer.get() );
            }
            return read;
        }
    }
}
