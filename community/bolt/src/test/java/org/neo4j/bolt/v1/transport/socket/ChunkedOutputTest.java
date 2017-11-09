/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.v1.transport.socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.bolt.v1.packstream.PackOutputClosedException;
import org.neo4j.bolt.v1.transport.ChunkedOutput;
import org.neo4j.kernel.impl.util.HexPrinter;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChunkedOutputTest
{
    private final Channel ch = mock( Channel.class );
    private final ByteBuffer writtenData = ByteBuffer.allocate( 1024 );
    private ChunkedOutput out;

    @Before
    public void setUp()
    {
        when( ch.alloc() ).thenReturn( UnpooledByteBufAllocator.DEFAULT );
        this.out = new ChunkedOutput( ch, 16 );
    }

    @After
    public void tearDown()
    {
        out.close();
    }

    @Test
    public void shouldNotNPE() throws Throwable
    {
        ExecutorService runner = Executors.newFixedThreadPool( 4 );
        // When
        runner.execute( () ->
        {
            try
            {
                for ( int i = 0; i < 5; i++ )
                {

                    out.writeByte( (byte) 1 ).writeShort( (short) 2 );
                    out.onMessageComplete();
                    out.flush();
                    Thread.sleep( ThreadLocalRandom.current().nextLong( 5 ) );

                }
            }
            catch ( IOException | InterruptedException e )
            {
                throw new AssertionError( e );
            }
        } );
        for ( int i = 0; i < 9; i++ )
        {
            runner.execute( () ->
            {
                try
                {
                    for ( int j = 0; j < 5; j++ )
                    {
                        out.flush();
                        Thread.sleep( ThreadLocalRandom.current().nextLong( 5 ) );
                    }
                }
                catch ( IOException | InterruptedException e )
                {
                    throw new AssertionError( e );
                }
            } );
        }

        runner.awaitTermination( 2, TimeUnit.SECONDS );
    }

    @Test
    public void shouldChunkSingleMessage() throws Throwable
    {
        setupWriteAndFlush();

        // When
        out.writeByte( (byte) 1 ).writeShort( (short) 2 );
        out.onMessageComplete();
        out.flush();

        // Then
        assertThat( writtenData.limit(), equalTo( 7 ) );
        assertThat( HexPrinter.hex( writtenData, 0, 7 ),
                equalTo( "00 03 01 00 02 00 00" ) );
    }

    @Test
    public void shouldChunkMessageSpanningMultipleChunks() throws Throwable
    {
        setupWriteAndFlush();

        // When
        out.writeLong( 1 ).writeLong( 2 ).writeLong( 3 );
        out.onMessageComplete();
        out.flush();

        // Then
        assertThat( writtenData.limit(), equalTo( 32 ) );
        assertThat( HexPrinter.hex( writtenData, 0, 32 ),
                equalTo( "00 08 00 00 00 00 00 00    00 01 00 08 00 00 00 00    " +
                         "00 00 00 02 00 08 00 00    00 00 00 00 00 03 00 00" ) );
    }

    @Test
    public void shouldReserveSpaceForChunkHeaderWhenWriteDataToNewChunk() throws IOException
    {
        setupWriteAndFlush();

        // Given 2 bytes left in buffer + chunk is closed
        out.writeBytes( new byte[10], 0, 10 );  // 2 (header) + 10
        out.onMessageComplete();                // 2 (ending)

        // When write 2 bytes
        out.writeShort( (short) 33 );           // 2 (header) + 2

        // Then the buffer should auto flash if space left (2) is smaller than new data and chunk header (2 + 2)
        assertThat( writtenData.limit(), equalTo( 14 ) );
        assertThat( HexPrinter.hex( writtenData, 0, 14 ),
                equalTo( "00 0A 00 00 00 00 00 00    00 00 00 00 00 00" ) );
    }

    @Test
    public void shouldChunkDataWhoseSizeIsGreaterThanOutputBufferCapacity() throws IOException
    {
        setupWriteAndFlush();

        // Given
        out.writeBytes( new byte[16], 0, 16 ); // 2 + 16 is greater than the default max size 16
        out.onMessageComplete();
        out.flush();

        // When & Then
        assertThat( writtenData.limit(), equalTo( 22 ) );
        assertThat( HexPrinter.hex( writtenData, 0, 22 ),
                equalTo( "00 0E 00 00 00 00 00 00    00 00 00 00 00 00 00 00    00 02 00 00 00 00" ) );
    }

    @Test
    public void shouldNotThrowIfOutOfSyncFlush() throws Throwable
    {
        setupWriteAndFlush();

        // When
        out.writeLong( 1 ).writeLong( 2 ).writeLong( 3 );
        out.onMessageComplete();
        out.flush();
        out.close();
        //this flush comes in to late but should not cause ChunkedOutput to choke.
        out.flush();

        // Then
        assertThat( writtenData.limit(), equalTo( 32 ) );
        assertThat( HexPrinter.hex( writtenData, 0, 32 ),
                equalTo( "00 08 00 00 00 00 00 00    00 01 00 08 00 00 00 00    " +
                         "00 00 00 02 00 08 00 00    00 00 00 00 00 03 00 00" ) );
    }

    @Test
    public void shouldQueueWritesMadeWhileFlushing() throws Throwable
    {
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        final CountDownLatch finishLatch = new CountDownLatch( 1 );
        final AtomicBoolean parallelException = new AtomicBoolean( false );

        when( ch.writeAndFlush( any(), isNull() ) ).thenAnswer( invocation ->
        {
            startLatch.countDown();
            ByteBuf byteBuf = invocation.getArgument( 0 );
            writtenData.limit( writtenData.position() + byteBuf.readableBytes() );
            byteBuf.readBytes( writtenData );
            return null;
        } );

        class ParallelWriter extends Thread
        {
            @Override
            public void run()
            {
                try
                {
                    startLatch.await();
                    out.writeShort( (short) 2 );
                    out.flush();
                }
                catch ( Exception e )
                {
                    e.printStackTrace( System.err );
                    parallelException.set( true );
                }
                finally
                {
                    finishLatch.countDown();
                }
            }
        }
        new ParallelWriter().start();

        // When
        out.writeShort( (short) 1 );
        out.flush();

        finishLatch.await();

        // Then
        assertFalse( parallelException.get() );
        assertThat( writtenData.limit(), equalTo( 8 ) );
        assertThat( HexPrinter.hex( writtenData, 0, 8 ), equalTo( "00 02 00 01 00 02 00 02" ) );
    }

    @Test
    public void shouldNotBeAbleToWriteAfterClose() throws Throwable
    {
        // When
        out.writeLong( 1 ).writeLong( 2 ).writeLong( 3 );
        out.onMessageComplete();
        out.flush();
        out.close();
        try
        {
            out.writeShort( (short) 42 );
            fail( "Should have thrown IOException" );
        }
        catch ( IOException e )
        {
            // We've been expecting you Mr Bond.
        }
    }

    @Test
    public void shouldFlushOnClose() throws Throwable
    {
        setupWriteAndFlush();

        // When
        out.writeLong( 1 ).writeLong( 2 ).writeLong( 3 );
        out.onMessageComplete();
        out.close();

        // Then
        assertThat( writtenData.limit(), equalTo( 32 ) );
        assertThat( HexPrinter.hex( writtenData, 0, 32 ),
                equalTo( "00 08 00 00 00 00 00 00    00 01 00 08 00 00 00 00    " +
                         "00 00 00 02 00 08 00 00    00 00 00 00 00 03 00 00" ) );
    }

    @Test
    public void shouldThrowErrorWithRemoteAddressWhenClosed() throws Exception
    {
        SocketAddress remoteAddress = mock( SocketAddress.class );
        String remoteAddressString = "client.server.com:7687";
        when( remoteAddress.toString() ).thenReturn( remoteAddressString );
        when( ch.remoteAddress() ).thenReturn( remoteAddress );

        out.close();

        try
        {
            out.writeInt( 42 );
        }
        catch ( PackOutputClosedException e )
        {
            assertThat( e.getMessage(), containsString( remoteAddressString ) );
        }
    }

    private void setupWriteAndFlush()
    {
        when( ch.writeAndFlush( any(), isNull() ) ).thenAnswer( invocation ->
        {
            ByteBuf byteBuf = invocation.getArgument( 0 );
            writtenData.limit( writtenData.position() + byteBuf.readableBytes() );
            byteBuf.readBytes( writtenData );
            return null;
        } );
    }
}
