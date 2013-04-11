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
package org.neo4j.kernel.impl.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Test;

public class TestBufferedFileChannel
{
    @Test
    public void testCorrectness() throws Exception
    {
        File file = createBigTempFile( 1 );
        FileChannel channel = new BufferedFileChannel( new RandomAccessFile( file, "r" ).getChannel() );
        ByteBuffer buffer = ByteBuffer.allocateDirect( 15 );
        int counter = 0;
        int loopCounter = 0;
        while ( channel.read( buffer ) != -1 )
        {
            buffer.flip();
            while ( buffer.hasRemaining() )
            {
                byte value = buffer.get();
                assertEquals( value, (byte)(counter%10) );
                counter++;
            }
            assertEquals( counter, channel.position() );
            int newLimit = loopCounter%buffer.capacity();
            buffer.clear().limit( newLimit == 0 ? 1 : newLimit );
            loopCounter++;
        }
        channel.close();
        file.delete();
    }
    
    @Test
    public void testPositioning() throws Exception
    {
        File file = createBigTempFile( 1 );
        FileChannel channel = new BufferedFileChannel( new RandomAccessFile( file, "r" ).getChannel() );
        ByteBuffer buffer = ByteBuffer.allocateDirect( 15 );
        
        channel.read( buffer );
        buffer.flip();
        for ( int value = 0; buffer.hasRemaining(); value++ )
        {
            assertEquals( value%10, buffer.get() );
        }
        
        buffer.clear();
        channel.position( channel.position()+5 );
        channel.read( buffer );
        buffer.flip();
        for ( int value = 0; buffer.hasRemaining(); value++ )
        {
            assertEquals( value%10, buffer.get() );
        }
        
        buffer.clear();
        channel.position( channel.size()-13 );
        channel.read( buffer );
        buffer.flip();
        for ( int value = 7; buffer.hasRemaining(); value++ )
        {
            assertEquals( value%10, buffer.get() );
        }
        
        channel.close();
        file.delete();
    }
    
//    @Test
//    public void testSpeed() throws Exception
//    {
//        File file = createBigTempFile( 5 );
//        long totalSlow = 0;
//        long totalFast = 0;
//        for ( int i = 0; i < 3; i++ )
//        {
//            long t = currentTimeMillis();
//            readOneByteAtATimeFromNakedChannel( file );
//            totalSlow += currentTimeMillis()-t;
//            t = currentTimeMillis();
//            readOneByteAtATimeFromBufferingChannel( file );
//            totalFast += currentTimeMillis()-t;
//        }
//        assertTrue( totalSlow / totalFast >= 5.0 );
//        file.delete();
//    }

//    private void readOneByteAtATimeFromNakedChannel( File file ) throws IOException
//    {
//        FileChannel channel = new RandomAccessFile( file, "r" ).getChannel();
//        readTheEntireChannel( channel );
//        channel.close();
//    }
//
//    private void readOneByteAtATimeFromBufferingChannel( File file ) throws IOException
//    {
//        FileChannel channel = new RandomAccessFile( file, "r" ).getChannel();
//        FileChannel buffering = new BufferedFileChannel( channel );
//        readTheEntireChannel( buffering );
//        channel.close();
//    }
//    
//    private void readTheEntireChannel( FileChannel buffering ) throws IOException
//    {
//        ByteBuffer buffer = ByteBuffer.wrap( new byte[4] );
//        while ( buffering.read( buffer ) != -1 )
//        {
//            buffer.clear();
//        }
//    }

    private File createBigTempFile( int mb ) throws IOException
    {
        File file = File.createTempFile( "neo4j", "temp" );
        file.deleteOnExit();
        FileChannel channel = new RandomAccessFile( file, "rw" ).getChannel();
        byte[] bytes = newStripedBytes( 1000 );
        ByteBuffer buffer = ByteBuffer.wrap( bytes );
        for ( int i = 0; i < 1000*mb; i++ )
        {
            buffer.clear();
            buffer.position( buffer.capacity() );
            buffer.flip();
            channel.write( buffer );
        }
        channel.close();
        return file;
    }

    private byte[] newStripedBytes( int size )
    {
        byte[] result = new byte[size];
        for ( int i = 0; i < size; i++ )
        {
            result[i] = (byte)(i%10);
        }
        return result;
    }
}
