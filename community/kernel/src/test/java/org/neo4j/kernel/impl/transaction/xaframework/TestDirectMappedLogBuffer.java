/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphdb.mockfs.BreakableFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.FileSystemGuard;
import org.neo4j.kernel.impl.nioneo.store.StoreFileChannel;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestDirectMappedLogBuffer
{
    class FileChannelWithChoppyDisk extends StoreFileChannel
    {

        ByteBuffer buff = ByteBuffer.allocate(1024);
        private int chunkSize;

        public FileChannelWithChoppyDisk(int writeThisMuchAtATime)
        {
            super( (FileChannel) null );
            this.chunkSize = writeThisMuchAtATime;
        }

        @Override
        public int write( ByteBuffer byteBuffer, long l ) throws IOException
        {
            int bytesToWrite = chunkSize > (byteBuffer.limit() - byteBuffer.position()) ?  byteBuffer.limit() - byteBuffer.position() : chunkSize;
                    buff.position( (int)l );

            // Remember original limit
            int originalLimit = byteBuffer.limit();

            // Set limit to not be bigger than chunk size
            byteBuffer.limit(byteBuffer.position() + bytesToWrite);

            // Write
            buff.put( byteBuffer );

            // Restore limit
            byteBuffer.limit(originalLimit);

            return bytesToWrite;
        }

        @Override
        public long position() throws IOException
        {
            return buff.position();
        }

        @Override
        public StoreFileChannel position( long l ) throws IOException
        {
            buff.position( (int) l );
            return this;
        }

        @Override
        public long size() throws IOException
        {
            return buff.capacity();
        }

        @Override
        public StoreFileChannel truncate( long l ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void force( boolean b ) throws IOException  { }
    }

    @Test
    public void shouldHandleDiskThatWritesOnlyTwoBytesAtATime() throws Exception
    {
        // Given
        FileChannelWithChoppyDisk mockChannel = new FileChannelWithChoppyDisk(/* that writes */2/* bytes at a time */);
        LogBuffer writeBuffer = new DirectMappedLogBuffer( mockChannel, new Monitors().newMonitor( ByteCounterMonitor.class ) );

        // When
        writeBuffer.put( new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16} );
        writeBuffer.writeOut();

        // Then
        assertThat(mockChannel.buff.position(  ), is(16));
    }

    @Test(expected = IOException.class)
    public void shouldFailIfUnableToWriteASingleByte() throws Exception
    {
        // Given
        FileChannelWithChoppyDisk mockChannel = new FileChannelWithChoppyDisk(/* that writes */0/* bytes at a time */);
        LogBuffer writeBuffer = new DirectMappedLogBuffer( mockChannel, new Monitors().newMonitor( ByteCounterMonitor.class ) );

        // When
        writeBuffer.put( new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16} );
        writeBuffer.writeOut();

        // Then expect an IOException
    }

    @Test
    @Ignore("This test demonstrates a way in which DirectMappedLogBuffer can fail. In particular, using DMLB after an" +
            "IOException can cause corruption in the underlying file channel. However, it is wrong to use DMLB after" +
            "such an error anyway, so this not something requiring fixing.")
    public void logBufferWritesContentsTwiceOnFailure() throws Exception
    {
        /*
         * The guard will throw an exception before writing the fifth byte. We will catch that and try to continue
         * writing. If that operation leads to writing to position 0 again then this is obviously an error (as we
         * will be overwriting the stuff we wrote before the exception) and so we must fail.
         */
        final AtomicBoolean broken = new AtomicBoolean( false );
        FileSystemGuard guard = new FileSystemGuard()
        {
            @Override
            public void checkOperation( OperationType operationType, File onFile, int bytesWrittenTotal,
                                        int bytesWrittenThisCall, long channelPosition ) throws IOException
            {
                if ( !broken.get() && bytesWrittenTotal == 4 )
                {
                    broken.set( true );
                    throw new IOException( "IOException after which this buffer should not be used" );
                }
                if ( broken.get() && channelPosition == 0 )
                {
                    throw new IOException( "This exception should never happen" );
                }
            }
        };

        BreakableFileSystemAbstraction fs = new BreakableFileSystemAbstraction( new EphemeralFileSystemAbstraction(), guard );
        DirectMappedLogBuffer buffer = new DirectMappedLogBuffer( fs.create( new File( "log" ) ), new Monitors().newMonitor( ByteCounterMonitor.class ) );
        buffer.putInt( 1 ).putInt( 2 ).putInt( 3 );
        try
        {
            buffer.writeOut();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        buffer.writeOut();
    }

    @Test
    public void testMonitoringBytesWritten() throws Exception
    {
        Monitors monitors = new Monitors();
        ByteCounterMonitor monitor = monitors.newMonitor( ByteCounterMonitor.class );
        DirectMappedLogBuffer buffer = new DirectMappedLogBuffer( new FileChannelWithChoppyDisk( 100 ), monitor );

        final AtomicLong bytesWritten = new AtomicLong();

        monitors.addMonitorListener( new ByteCounterMonitor()
        {
            @Override
            public void bytesWritten( long numberOfBytes )
            {
                bytesWritten.addAndGet( numberOfBytes );
            }

            @Override
            public void bytesRead( long numberOfBytes )
            {

            }
        } );

        buffer.put( (byte) 1 );
        assertEquals( 0, bytesWritten.get() );
        buffer.force();
        assertEquals( 1, bytesWritten.get() );

        buffer.putShort( (short) 1 );
        assertEquals( 1, bytesWritten.get() );
        buffer.force();
        assertEquals( 3, bytesWritten.get() );

        buffer.putInt( 1 );
        assertEquals( 3, bytesWritten.get() );
        buffer.force();
        assertEquals( 7, bytesWritten.get() );

        buffer.putLong( 1 );
        assertEquals( 7, bytesWritten.get() );
        buffer.force();
        assertEquals( 15, bytesWritten.get() );

        buffer.putFloat( 1 );
        assertEquals( 15, bytesWritten.get() );
        buffer.force();
        assertEquals( 19, bytesWritten.get() );

        buffer.putDouble( 1 );
        assertEquals( 19, bytesWritten.get() );
        buffer.force();
        assertEquals( 27, bytesWritten.get() );

        buffer.put( new byte[]{ 1, 2, 3 } );
        assertEquals( 27, bytesWritten.get() );
        buffer.force();
        assertEquals( 30, bytesWritten.get() );

        buffer.put( new char[] { '1', '2', '3'} );
        assertEquals( 30, bytesWritten.get() );
        buffer.force();
        assertEquals( 36, bytesWritten.get() );

        buffer.force();
        assertEquals( 36, bytesWritten.get() );
    }
}
