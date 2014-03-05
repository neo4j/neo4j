/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandReader;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandWriter;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

/**
 * At any point, a power outage may stop us from writing to the log, which means that, at any point, all our commands
 * need to be able to handl the log ending mid-way through reading it.
 */
public class LogTruncationTest
{
    InMemoryLogBuffer inMemoryBuffer = new InMemoryLogBuffer();
    PhysicalLogNeoXaCommandReader reader = new PhysicalLogNeoXaCommandReader( ByteBuffer.allocate( 100 ) );
    PhysicalLogNeoXaCommandWriter writer = new PhysicalLogNeoXaCommandWriter();

    @Test
    public void testSerializationInFaceOfLogTruncation() throws Exception
    {
        // TODO: add support for other commands and permutations as well...
        Command.NodeCommand nodeCommand = new Command.NodeCommand();
        nodeCommand.init( new NodeRecord( 12l, false, 13l, 13l ),
                new NodeRecord( 0,false, 0,0 ) );
        assertHandlesLogTruncation( nodeCommand );
        Command.LabelTokenCommand labelTokenCommand = new Command.LabelTokenCommand();
        labelTokenCommand.init( new LabelTokenRecord( 1 ) );
        assertHandlesLogTruncation( labelTokenCommand );

        Command.NeoStoreCommand neoStoreCommand = new Command.NeoStoreCommand();
        neoStoreCommand.init( new NeoStoreRecord() );
        assertHandlesLogTruncation( neoStoreCommand );
//        assertHandlesLogTruncation( new Command.PropertyCommand( null,
//                new PropertyRecord( 1, true, new NodeRecord(1, 12, 12, true) ),
//                new PropertyRecord( 1, true, new NodeRecord(1, 12, 12, true) ) ) );
    }

    private void assertHandlesLogTruncation( XaCommand cmd ) throws IOException
    {
        inMemoryBuffer.reset();

        writer.write( cmd,inMemoryBuffer );

        int bytesSuccessfullyWritten = inMemoryBuffer.bytesWritten();
        assertEquals( cmd, reader.read( inMemoryBuffer ));

        bytesSuccessfullyWritten--;

        while(bytesSuccessfullyWritten --> 0)
        {
            inMemoryBuffer.reset();
            writer.write( cmd, inMemoryBuffer );
            inMemoryBuffer.truncateTo( bytesSuccessfullyWritten );

            XaCommand deserialized = reader.read( inMemoryBuffer );

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

        @Override
        public LogBuffer put( byte b ) throws IOException
        {
            ensureArrayCapacityPlus( 1 );
            bytes[writeIndex++] = b;
            return this;
        }

        @Override
        public LogBuffer putShort( short s ) throws IOException
        {
            ((ByteBuffer) bufferForConversions.clear()).putShort( s );
            return flipAndPut();
        }

        @Override
        public LogBuffer putInt( int i ) throws IOException
        {
            ((ByteBuffer) bufferForConversions.clear()).putInt( i );
            return flipAndPut();
        }

        @Override
        public LogBuffer putLong( long l ) throws IOException
        {
            ((ByteBuffer) bufferForConversions.clear()).putLong( l );
            return flipAndPut();
        }

        @Override
        public LogBuffer putFloat( float f ) throws IOException
        {
            ((ByteBuffer) bufferForConversions.clear()).putFloat( f );
            return flipAndPut();
        }

        @Override
        public LogBuffer putDouble( double d ) throws IOException
        {
            ((ByteBuffer) bufferForConversions.clear()).putDouble( d );
            return flipAndPut();
        }

        @Override
        public LogBuffer put( byte[] bytes ) throws IOException
        {
            ensureArrayCapacityPlus( bytes.length );
            System.arraycopy( bytes, 0, this.bytes, writeIndex, bytes.length );
            writeIndex += bytes.length;
            return this;
        }

        @Override
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

        @Override
        public void force() throws IOException
        {
        }

        @Override
        public long getFileChannelPosition() throws IOException
        {
            return this.readIndex;
        }

        @Override
        public FileChannel getFileChannel()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }

        @Override
        public void close() throws IOException
        {
        }

        @Override
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