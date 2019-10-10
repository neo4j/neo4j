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
package org.neo4j.metatest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.io.memory.ByteBuffers.allocate;

@ExtendWith( EphemeralFileSystemExtension.class )
class TestEphemeralFileChannel
{

    @Inject
    private EphemeralFileSystemAbstraction fileSystem;

    @Test
    void smoke() throws Exception
    {
        StoreChannel channel = fileSystem.write( new File( "yo" ) );

        // Clear it because we depend on it to be zeros where we haven't written
        ByteBuffer buffer = allocate( 23 );
        buffer.put( new byte[23] ); // zeros
        buffer.flip();
        channel.write( buffer );
        channel = fileSystem.write( new File( "yo" ) );
        long longValue = 1234567890L;

        // [1].....[2]........[1234567890L]...

        buffer.clear();
        buffer.limit( 1 );
        buffer.put( (byte) 1 );
        buffer.flip();
        channel.write( buffer );

        buffer.clear();
        buffer.limit( 1 );
        buffer.put( (byte) 2 );
        buffer.flip();
        channel.position( 6 );
        channel.write( buffer );

        buffer.clear();
        buffer.limit( 8 );
        buffer.putLong( longValue );
        buffer.flip();
        channel.position( 15 );
        channel.write( buffer );
        assertEquals( 23, channel.size() );

        // Read with position
        // byte 0
        buffer.clear();
        buffer.limit( 1 );
        channel.read( buffer, 0 );
        buffer.flip();
        assertEquals( (byte) 1, buffer.get() );

        // bytes 5-7
        buffer.clear();
        buffer.limit( 3 );
        channel.read( buffer, 5 );
        buffer.flip();
        assertEquals( (byte) 0, buffer.get() );
        assertEquals( (byte) 2, buffer.get() );
        assertEquals( (byte) 0, buffer.get() );

        // bytes 15-23
        buffer.clear();
        buffer.limit( 8 );
        channel.read( buffer, 15 );
        buffer.flip();
        assertEquals( longValue, buffer.getLong() );
    }

    @Test
    void absoluteVersusRelative() throws Exception
    {
        // GIVEN
        File file = new File( "myfile" );
        StoreChannel channel = fileSystem.write( file );
        byte[] bytes = "test".getBytes();
        channel.write( ByteBuffer.wrap( bytes ) );
        channel.close();

        // WHEN
        channel = fileSystem.read( new File( file.getAbsolutePath() ) );
        byte[] readBytes = new byte[bytes.length];
        channel.readAll( ByteBuffer.wrap( readBytes ) );

        // THEN
        assertArrayEquals( bytes, readBytes );
    }

    @Test
    void listFiles() throws Exception
    {
        /* GIVEN
         *                        root
         *                       /    \
         *         ----------- dir1   dir2
         *        /       /     |       \
         *    subdir1  file  file2      file
         *       |
         *     file
         */
        File root = new File( "/root" ).getCanonicalFile();
        File dir1 = new File( root, "dir1" );
        File dir2 = new File( root, "dir2" );
        File subdir1 = new File( dir1, "sub" );
        File file1 = new File( dir1, "file" );
        File file2 = new File( dir1, "file2" );
        File file3 = new File( dir2, "file" );
        File file4 = new File( subdir1, "file" );

        fileSystem.mkdirs( dir2 );
        fileSystem.mkdirs( dir1 );
        fileSystem.mkdirs( subdir1 );

        fileSystem.write( file1 );
        fileSystem.write( file2 );
        fileSystem.write( file3 );
        fileSystem.write( file4 );

        // THEN
        assertEquals( asSet( dir1, dir2 ), asSet( fileSystem.listFiles( root ) ) );
        assertEquals( asSet( subdir1, file1, file2 ), asSet( fileSystem.listFiles( dir1 ) ) );
        assertEquals( asSet( file3 ), asSet( fileSystem.listFiles( dir2 ) ) );
        assertEquals( asSet( file4 ), asSet( fileSystem.listFiles( subdir1 ) ) );
    }
}
