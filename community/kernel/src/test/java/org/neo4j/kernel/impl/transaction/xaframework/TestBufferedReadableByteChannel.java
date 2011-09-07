/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBufferedReadableByteChannel
{
    private File testFileObject;
    private RandomAccessFile testRAFile;

    @Before
    public void createFiles() throws Exception
    {
        File testDirectory = new File( "target" + File.separator + "var" );
        if ( !testDirectory.exists() )
        {
            testDirectory.mkdirs();
        }
        testFileObject = new File( testDirectory,
                "bufferedReadableFileByteChannelTest" );
        testRAFile = new RandomAccessFile( testFileObject, "rw" );
    }

    @After
    public void deleteFiles() throws Exception
    {
        testRAFile.close();
        testFileObject.delete();
    }

    @Test
    public void testBasicCreationAndOps() throws Exception
    {
        FileChannel channel = createFromData( new int[] { 1, 2, 3 }, new int[] {
                4, 5, 6 } );
        /*
         * Data is supposed to be now
         *      [1,2,3][4,5,6]
         *              ^(12)
         * the first bracket is the file, the second the buffer, the arrow the location.
         * Buffer should not be touched from now on, otherwise contents will change.
         * Use the readBuffer instead.
         */

        // After creation, the channel has the position of the file channel
        assertEquals( 12, channel.position() );
        // Repositioning must work
        channel.position( 5 );
        assertEquals( 5, channel.position() );
        channel.position( 6 );
        assertEquals( 6, channel.position() );
        channel.position( 0 );
        assertEquals( 0, channel.position() );

        ByteBuffer readBuffer = ByteBuffer.allocate( 16 );
        // Read some stuff in without changing position
        assertEquals( readBuffer.capacity(), channel.read( readBuffer, 4 ) );
        assertEquals( 0, channel.position() );
        readBuffer.flip();
        assertEquals( 2, readBuffer.getInt() );
        assertEquals( 3, readBuffer.getInt() );
        assertEquals( 4, readBuffer.getInt() );
        assertEquals( 5, readBuffer.getInt() );

        // And now read some overlapping stuff in changing position
        readBuffer.flip();
        assertEquals( readBuffer.capacity(), channel.read( readBuffer ) );
        assertEquals( readBuffer.capacity(), channel.position() );
        readBuffer.flip();
        assertEquals( 1, readBuffer.getInt() );
        assertEquals( 2, readBuffer.getInt() );
        assertEquals( 3, readBuffer.getInt() );
        assertEquals( 4, readBuffer.getInt() );
    }

    @Test
    public void testReadAtBoundaries() throws Exception
    {
        FileChannel channel = createFromData( new int[] { 1, 2, 3 }, new int[] {
                4, 5, 6 } );

        ByteBuffer readBuffer = ByteBuffer.allocate( 12 );
        assertEquals( readBuffer.capacity(), channel.read( readBuffer ) );
        assertEquals( 24, channel.position() );
        readBuffer.flip();
        assertEquals( 4, readBuffer.getInt() );
        assertEquals( 5, readBuffer.getInt() );
        assertEquals( 6, readBuffer.getInt() );

        readBuffer.flip();
        assertEquals( -1, channel.read( readBuffer ) );
    }

    @Test
    public void testWithEmptyFile() throws Exception
    {
        FileChannel channel = createFromData( new int[] {},
                new int[] { 4, 5, 6 } );

        ByteBuffer readBuffer = ByteBuffer.allocate( 12 );
        assertEquals( readBuffer.capacity(), channel.read( readBuffer ) );
        assertEquals( 12, channel.position() );
        readBuffer.flip();
        assertEquals( 4, readBuffer.getInt() );
        assertEquals( 5, readBuffer.getInt() );
        assertEquals( 6, readBuffer.getInt() );

        readBuffer.flip();
        assertEquals( -1, channel.read( readBuffer ) );
    }

    @Test
    public void testWithEmptyBuffer() throws Exception
    {
        FileChannel channel = createFromData( new int[] { 1, 2, 3 },
                new int[] {} );

        ByteBuffer readBuffer = ByteBuffer.allocate( 12 );
        assertEquals( -1, channel.read( readBuffer ) );

        channel.position(0);
        assertEquals( readBuffer.capacity(), channel.read( readBuffer ) );
        assertEquals( 12, channel.position() );
        readBuffer.flip();
        assertEquals( 1, readBuffer.getInt() );
        assertEquals( 2, readBuffer.getInt() );
        assertEquals( 3, readBuffer.getInt() );
    }

    private FileChannel createFromData( int[] inFile, int[] inBuffer )
            throws IOException
    {
        FileChannel channel;// A channel to play with
        ByteBuffer buffer; // A buffer to play with

        channel = testRAFile.getChannel();
        // Create max bsized buffer needed
        if ( inFile.length > 0 )
        {
            buffer = ByteBuffer.allocate( inFile.length * 4 );
            for ( int datum : inFile )
            {
                buffer.putInt( datum );
            }
            buffer.flip();
            channel.write( buffer );
            buffer.rewind();
        }

        if ( inBuffer.length > 0 )
        {
            buffer = ByteBuffer.allocate(inBuffer.length * 4);
            for ( int datum : inBuffer )
            {
                buffer.putInt( datum );
            }
        }
        else
        {
            buffer = ByteBuffer.allocate(0);
        }
        buffer.rewind();
        channel = new BufferedReadableByteChannel( channel, CloseableByteBuffer.wrap(buffer));
        return channel;
    }
}
