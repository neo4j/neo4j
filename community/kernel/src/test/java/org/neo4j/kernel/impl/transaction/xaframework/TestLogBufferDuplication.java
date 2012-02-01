/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Test;
import org.neo4j.kernel.CommonFactories;

public class TestLogBufferDuplication
{
    @Test
    public void testStandAloneSanity()
    {
        ByteBuffer original = ByteBuffer.allocate( 20 );
        original.putInt( 1 ).putInt( 2 );

        CloseableByteBuffer closeableOriginal = CloseableByteBuffer.wrap( original );
        closeableOriginal.flip();
        assertEquals( 1, closeableOriginal.getInt() );
        assertEquals( 2, original.getInt() );
        assertEquals( 8, closeableOriginal.position() );

        assertEquals( 8, closeableOriginal.limit() );
        closeableOriginal.limit( 20 ).position( 8 );
        original.putInt( 3 );
        closeableOriginal.putInt( 4 ).flip();

        CloseableByteBuffer dup = closeableOriginal.duplicate();
        assertEquals( 1, dup.getInt() );
        closeableOriginal.close();
        assertEquals( 2, dup.getInt() );
        assertEquals( 3, dup.getInt() );
        assertEquals( 4, dup.getInt() );
    }

    @Test
    public void testDuplication()
    {
        CloseableByteBuffer original = CloseableByteBuffer.wrap( ByteBuffer.allocate( 20 ) );
        original.putInt( 1 ).putInt( 2 ).putInt( 3 );
        original.flip();
        assertEquals( 1, original.getInt() );

        CloseableByteBuffer dup = original.duplicate();
        assertEquals( 2, dup.getInt() );
        dup.rewind();
        assertEquals( 1, dup.getInt() );
        assertEquals( 2, dup.getInt() );
        assertEquals( 3, dup.getInt() );

        assertEquals( 2, original.getInt() );
        assertEquals( 3, original.getInt() );

        // Overwrite and check data
        dup.rewind().putInt( 4 ).putInt( 5 );
        dup.rewind();
        assertEquals( 4, dup.getInt() );
        assertEquals( 5, dup.getInt() );

        original.rewind();
        assertEquals( 4, original.getInt() );
        assertEquals( 5, original.getInt() );

        /*
         * So far we have made sure that under duplication the contents are the same,
         * the state starts off the same but changes independently and on change the
         * contents are reflected which means that no data copy happened. Now we must make sure
         * that the copy happens on clear()
         */
        original.clear();

        original.putInt( 6 ).putInt( 7 ).putInt( 8 );

        dup.rewind();
        assertEquals( 4, dup.getInt() );
        assertEquals( 5, dup.getInt() );

        original.flip();
        assertEquals( 6, original.getInt() );
        assertEquals( 7, original.getInt() );
        assertEquals( 8, original.getInt() );
    }

    @Test
    public void testThroughLogBuffer() throws Exception
    {
        LogBufferFactory fac = CommonFactories.defaultLogBufferFactory();

        FileChannel channel = new RandomAccessFile( "target/var/foobar2", "rw" ).getChannel();

        LogBuffer buffer = fac.create( channel );
        assertEquals( DirectMappedLogBuffer.class, buffer.getClass() );

        buffer.putInt( 1 ).putInt( 2 ).putInt( 3 );

        // Buffer is now not empty, time to duplicate
        FileChannel channel2 = new RandomAccessFile( "target/var/foobar3", "rw" ).getChannel();
        FileChannel dup = fac.combine( channel2, buffer );

        ByteBuffer readInMe = ByteBuffer.allocate( 4 ); // One integers
        dup.read( readInMe );
        readInMe.flip();
        assertEquals( 1, readInMe.getInt() );
        readInMe.flip();

        buffer.force();

        dup.read( readInMe );
        readInMe.flip();
        assertEquals( 2, readInMe.getInt() );
        readInMe.flip();

        buffer.putInt( 4 ).putInt( 5 ).putInt( 6 );

        dup.read( readInMe );
        readInMe.flip();
        assertEquals( 3, readInMe.getInt() );
        readInMe.flip();

        dup.close();

        buffer.force();
    }

    @Test
    public void testBigWrite() throws Exception
    {
        LogBufferFactory fac = CommonFactories.defaultLogBufferFactory();

        FileChannel channel = new RandomAccessFile( "target/var/foobar4", "rw" ).getChannel();

        LogBuffer buffer = fac.create( channel );
        assertEquals( DirectMappedLogBuffer.class, buffer.getClass() );

        int writesAtFirst = DirectMappedLogBuffer.BUFFER_SIZE / 4; // Max
        // size
        for ( int i = 0; i < writesAtFirst; i++ )
        {
            buffer.putInt( i );
        }

        // Buffer is now not empty, time to duplicate
        FileChannel channel2 = new RandomAccessFile( "target/var/foobar5", "rw" ).getChannel();
        FileChannel dup = fac.combine( channel2, buffer );

        // dup is now locked at the latest contents. Will check after we force a
        // flush
        for ( int i = 0; i < 1; i++ )
        {
            buffer.putInt( i );
        }

        ByteBuffer readInMe = ByteBuffer.allocate( writesAtFirst*4 );
        dup.read( readInMe );
        readInMe.flip();
        for ( int i = 0; i < writesAtFirst; i++ )
        {
            assertEquals( i, readInMe.getInt() );
        }
    }
}
