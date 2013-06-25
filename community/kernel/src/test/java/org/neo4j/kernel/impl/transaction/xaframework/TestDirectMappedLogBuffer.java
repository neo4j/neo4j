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
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.junit.Test;

public class TestDirectMappedLogBuffer
{

    class FileChannelWithChoppyDisk extends FileChannel
    {

        ByteBuffer buff = ByteBuffer.allocate(1024);
        private int chunkSize;

        public FileChannelWithChoppyDisk(int writeThisMuchAtATime)
        {
            this.chunkSize = writeThisMuchAtATime;
        }

        @Override
        public int read( ByteBuffer byteBuffer ) throws IOException
        { return 0; }

        @Override
        public long read( ByteBuffer[] byteBuffers, int i, int i1 ) throws IOException
        { return 0; }

        @Override
        public int write( ByteBuffer byteBuffer ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long write( ByteBuffer[] byteBuffers, int i, int i1 ) throws IOException
        {
            throw new UnsupportedOperationException();
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
        public FileChannel position( long l ) throws IOException
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
        public FileChannel truncate( long l ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void force( boolean b ) throws IOException  { }

        @Override
        public long transferTo( long l, long l1, WritableByteChannel writableByteChannel ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long transferFrom( ReadableByteChannel readableByteChannel, long l, long l1 ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read( ByteBuffer byteBuffer, long l ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public MappedByteBuffer map( MapMode mapMode, long l, long l1 ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileLock lock( long l, long l1, boolean b ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileLock tryLock( long l, long l1, boolean b ) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void implCloseChannel() throws IOException
        {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void shouldHandleDiskThatWritesOnlyTwoBytesAtATime() throws Exception
    {
        // Given
        FileChannelWithChoppyDisk mockChannel = new FileChannelWithChoppyDisk(/* that writes */2/* bytes at a time */);
        LogBuffer writeBuffer = new DirectMappedLogBuffer( mockChannel );

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
        LogBuffer writeBuffer = new DirectMappedLogBuffer( mockChannel );

        // When
        writeBuffer.put( new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16} );
        writeBuffer.writeOut();

        // Then expect an IOException
    }
}
