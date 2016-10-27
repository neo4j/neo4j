/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedNioStream;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LimitedLengthReadableByteChannelTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void shouldLimitSize() throws Exception
    {
        // given
        File sourceFile = sourceFileWithBytes( 1024 );
        File destinationFile = testDirectory.file( "destination" );

        // when
        int bytesToRead = 500;
        LimitedLengthReadableByteChannel readChannel = new LimitedLengthReadableByteChannel( new FileInputStream( sourceFile ).getChannel(), bytesToRead );
        ByteBuffer byteBuffer = ByteBuffer.allocate( 32 );

        FileChannel writeChannel = new FileOutputStream( destinationFile ).getChannel();
        for (;;)
        {
            int read = readChannel.read( byteBuffer );
            if ( read < 0 )
            {
                break;
            }

            byteBuffer.flip();
            writeChannel.write( byteBuffer );
            byteBuffer.clear();
        }

        // then
        assertEquals( bytesToRead, destinationFile.length() );
    }

    @Test
    public void shouldReadWhenBufferEmptyAndMoreFileToRead() throws Exception
    {
        // given
        File sourceFile = sourceFileWithBytes( 32 );
        File destinationFile = testDirectory.file( "destination" );

        // when
        int bytesToRead = 32;
        LimitedLengthReadableByteChannel readChannel = new LimitedLengthReadableByteChannel( new
                FileInputStream( sourceFile ).getChannel(), bytesToRead );

        ByteBuffer byteBuffer = ByteBuffer.allocate( 32 );

        // then fills the whole buffer
        FileChannel writeChannel = new FileOutputStream( destinationFile ).getChannel();
        assertEquals( 32, readChannel.read( byteBuffer ) );

        // then writes to a file
        byteBuffer.flip();
        writeChannel.write( byteBuffer );
        assertEquals( bytesToRead, destinationFile.length() );
    }

    @Test
    public void shouldNotReadWhenNoMoreFileToRead() throws Exception
    {
        // given
        File sourceFile = sourceFileWithBytes( 32 );

        // when
        int bytesToRead = 32;
        LimitedLengthReadableByteChannel readChannel = new LimitedLengthReadableByteChannel( new
                FileInputStream( sourceFile ).getChannel(), bytesToRead );

        ByteBuffer byteBuffer = ByteBuffer.allocate( 32 );

        // read the whole file
        readChannel.read( byteBuffer );

        // then read with full buffer
        assertEquals( -1, readChannel.read( byteBuffer ) );

        // then read with empty buffer
        byteBuffer.clear();
        assertEquals( -1, readChannel.read( byteBuffer ) );
    }

    @Test
    public void shouldSomethingWhenBufferNotFull() throws Exception
    {
        // given
        File sourceFile = sourceFileWithBytes( 48 );

        // when
        int bytesToRead = 48;
        LimitedLengthReadableByteChannel readChannel = new LimitedLengthReadableByteChannel( new
                FileInputStream( sourceFile ).getChannel(), bytesToRead );

        ByteBuffer byteBuffer = ByteBuffer.allocate( 32 );

        // read the first bit of the file
        assertEquals(32, readChannel.read( byteBuffer ) );

        // buffer full but still file to read
        assertEquals(0, readChannel.read( byteBuffer ) );

        // read the rest of the file
        byteBuffer.clear();
        assertEquals(16, readChannel.read( byteBuffer ) );

        // then nothing else to read
        assertEquals(-1, readChannel.read( byteBuffer ) );
    }

    @Test
    public void shouldReadLikeAChunkedNioStream() throws Exception
    {
        // given
        File sourceFile = sourceFileWithBytes( 1024 );
        File destinationFile = testDirectory.file( "destination" );

        // when
        int bytesToRead = 33;
        LimitedLengthReadableByteChannel jc = new LimitedLengthReadableByteChannel( new FileInputStream( sourceFile ).getChannel(), bytesToRead );

        ChunkedNioStream chunks = new ChunkedNioStream( jc, 32 );

        // when
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );
        when( context.alloc() ).thenReturn( new UnpooledByteBufAllocator( false ) );

        FileOutputStream outputStream = new FileOutputStream( destinationFile );

        while (true)
        {
            ByteBuf message = chunks.readChunk( context );

            if(message == null)
            {
                break;
            }
            message.readBytes( outputStream, message.readableBytes() );

            boolean endOfInput = chunks.isEndOfInput();
            if (endOfInput)
            {
                break;
            }

        }

        // then
        assertEquals( bytesToRead, destinationFile.length() );
    }

    private File sourceFileWithBytes( int numberOfBytes ) throws IOException
    {
        File sourceFile = testDirectory.file( "source" );
        byte[] bytes = new byte[numberOfBytes];
        for ( int i = 0; i < numberOfBytes; i++ )
        {
            bytes[i] = (byte) i;
        }
        Files.write( sourceFile.toPath(), bytes );
        return sourceFile;
    }
}
