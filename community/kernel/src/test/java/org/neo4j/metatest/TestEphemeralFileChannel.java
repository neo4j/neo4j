/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.metatest;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;

import static java.nio.ByteBuffer.allocateDirect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class TestEphemeralFileChannel
{
    @Test
    public void smoke() throws Exception
    {
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        StoreChannel channel = fs.open( new File( "yo" ), "rw" );
        
        // Clear it because we depend on it to be zeros where we haven't written
        ByteBuffer buffer = allocateDirect( 23 );
        buffer.put( new byte[23] ); // zeros
        buffer.flip();
        channel.write( buffer );
        channel = fs.open( new File("yo"), "rw" );
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
        fs.shutdown();
    }
    
    @Test
    @Ignore
    public void absoluteVersusRelative() throws Exception
    {
        // GIVEN
        File file = new File( "myfile" );
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        StoreChannel channel = fs.open( file, "rw" );
        byte[] bytes = "test".getBytes();
        channel.write( ByteBuffer.wrap( bytes ) );
        channel.close();

        // WHEN
        channel = fs.open( new File( file.getAbsolutePath() ), "r" );
        byte[] readBytes = new byte[bytes.length];
        int nrOfReadBytes = channel.read( ByteBuffer.wrap( readBytes ) );

        // THEN
        assertEquals( bytes.length, nrOfReadBytes );
        assertTrue( Arrays.equals( bytes, readBytes ) );
        fs.shutdown();
    }
    
    @Test
    public void listFiles() throws Exception
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
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        File root = new File( "root" );
        File dir1 = new File( root, "dir1" );
        File dir2 = new File( root, "dir2" );
        File subdir1 = new File( dir1, "sub" );
        File file1 = new File( dir1, "file" );
        File file2 = new File( dir1, "file2" );
        File file3 = new File( dir2, "file" );
        File file4 = new File( subdir1, "file" );
        
        fs.mkdirs( dir2 );
        fs.mkdirs( dir1 );
        fs.mkdirs( subdir1 );
        
        fs.create( file1 );
        fs.create( file2 );
        fs.create( file3 );
        fs.create( file4 );

        // THEN
        assertEquals( asSet( dir1, dir2 ), asSet( fs.listFiles( root ) ) );
        assertEquals( asSet( subdir1, file1, file2 ), asSet( fs.listFiles( dir1 ) ) );
        assertEquals( asSet( file3 ), asSet( fs.listFiles( dir2 ) ) );
        assertEquals( asSet( file4 ), asSet( fs.listFiles( subdir1 ) ) );
        fs.shutdown();
    }
}
