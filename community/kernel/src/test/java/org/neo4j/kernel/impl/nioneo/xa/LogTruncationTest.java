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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.junit.Test;

import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;

/**
 * At any point, a power outage may stop us from writing to the log, which means that, at any point, all our commands
 * need to be able to handl the log ending mid-way through reading it.
 */
public class LogTruncationTest
{
    InMemoryLogBuffer inMemoryBuffer = new InMemoryLogBuffer();

    @Test
    public void testSerializationInFaceOfLogTruncation() throws Exception
    {
        // TODO: add support for other commands and permutations as well...
        assertHandlesLogTruncation( new Command.NodeCommand( null,
                                                             new NodeRecord( 12l, 13l, 13l ),
                                                             new NodeRecord( 0,0,0 ) ) );
        assertHandlesLogTruncation( new Command.LabelTokenCommand( null, new LabelTokenRecord( 1 )) );

        assertHandlesLogTruncation( new Command.NeoStoreCommand( null, new NeoStoreRecord() ) );
//        assertHandlesLogTruncation( new Command.PropertyCommand( null,
//                new PropertyRecord( 1, true, new NodeRecord(1, 12, 12, true) ),
//                new PropertyRecord( 1, true, new NodeRecord(1, 12, 12, true) ) ) );
    }

    private void assertHandlesLogTruncation( XaCommand cmd ) throws IOException
    {
        inMemoryBuffer.reset();
        cmd.writeToFile( inMemoryBuffer );

        int bytesSuccessfullyWritten = inMemoryBuffer.bytesWritten();
        assertEquals( cmd, Command.readCommand( null, null, inMemoryBuffer, ByteBuffer.allocate( 100 ) ));

        bytesSuccessfullyWritten--;

        while(bytesSuccessfullyWritten --> 0)
        {
            inMemoryBuffer.reset();
            cmd.writeToFile( inMemoryBuffer );
            inMemoryBuffer.truncateTo( bytesSuccessfullyWritten );

            Command deserialized = Command.readCommand( null, null, inMemoryBuffer, ByteBuffer.allocate( 100 ) );

            assertNull( "Deserialization did not detect log truncation! Record: " + cmd +
                        ", deserialized: " + deserialized, deserialized );
        }
    }

    public class InMemoryLogBuffer implements LogBuffer, ReadableByteChannel
    {
        private byte[] bytes = new byte[1000];
        private int writeIndex;
        private int readIndex;
        private ByteBuffer bufferForConversions = ByteBuffer.wrap( new byte[100] );

        public InMemoryLogBuffer()
        {
        }

        public void reset()
        {
            writeIndex = readIndex = 0;
        }

        public void truncateTo( int bytes )
        {
            writeIndex = bytes;
        }

        public int bytesWritten()
        {
            return writeIndex;
        }

        private void ensureArrayCapacityPlus( int plus )
        {
            while ( writeIndex+plus > bytes.length )
            {
                byte[] tmp = bytes;
                bytes = new byte[bytes.length*2];
                System.arraycopy( tmp, 0, bytes, 0, tmp.length );
            }
        }

        private LogBuffer flipAndPut()
        {
            ensureArrayCapacityPlus( bufferForConversions.limit() );
            System.arraycopy( bufferForConversions.flip().array(), 0, bytes, writeIndex,
                              bufferForConversions.limit() );
            writeIndex += bufferForConversions.limit();
            return this;
        }

        public LogBuffer put( byte b ) throws IOException
        {
            ensureArrayCapacityPlus( 1 );
            bytes[writeIndex++] = b;
            return this;
        }

        public LogBuffer putShort( short s ) throws IOException
        {
            ((ByteBuffer) bufferForConversions.clear()).putShort( s );
            return flipAndPut();
        }

        public LogBuffer putInt( int i ) throws IOException
        {
            ((ByteBuffer) bufferForConversions.clear()).putInt( i );
            return flipAndPut();
        }

        public LogBuffer putLong( long l ) throws IOException
        {
            ((ByteBuffer) bufferForConversions.clear()).putLong( l );
            return flipAndPut();
        }

        public LogBuffer putFloat( float f ) throws IOException
        {
            ((ByteBuffer) bufferForConversions.clear()).putFloat( f );
            return flipAndPut();
        }

        public LogBuffer putDouble( double d ) throws IOException
        {
            ((ByteBuffer) bufferForConversions.clear()).putDouble( d );
            return flipAndPut();
        }

        public LogBuffer put( byte[] bytes ) throws IOException
        {
            ensureArrayCapacityPlus( bytes.length );
            System.arraycopy( bytes, 0, this.bytes, writeIndex, bytes.length );
            writeIndex += bytes.length;
            return this;
        }

        public LogBuffer put( char[] chars ) throws IOException
        {
            ensureConversionBufferCapacity( chars.length*2 );
            bufferForConversions.clear();
            for ( char ch : chars )
            {
                bufferForConversions.putChar( ch );
            }
            return flipAndPut();
        }

        private void ensureConversionBufferCapacity( int length )
        {
            if ( bufferForConversions.capacity() < length )
            {
                bufferForConversions = ByteBuffer.wrap( new byte[length*2] );
            }
        }

        @Override
        public void writeOut() throws IOException
        {
        }

        public void force() throws IOException
        {
        }

        public long getFileChannelPosition() throws IOException
        {
            return this.readIndex;
        }

        public StoreChannel getFileChannel()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isOpen()
        {
            return true;
        }

        public void close() throws IOException
        {
        }

        public int read( ByteBuffer dst ) throws IOException
        {
            if ( readIndex >= writeIndex )
            {
                return -1;
            }

            int actualLengthToRead = Math.min( dst.limit(), writeIndex-readIndex );
            try
            {
                dst.put( bytes, readIndex, actualLengthToRead );
                return actualLengthToRead;
            }
            finally
            {
                readIndex += actualLengthToRead;
            }
        }
    }
}
