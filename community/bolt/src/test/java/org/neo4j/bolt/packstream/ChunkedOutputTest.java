/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.packstream;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.neo4j.io.memory.ByteBuffers;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.packstream.ChunkedOutput.CHUNK_HEADER_SIZE;
import static org.neo4j.bolt.transport.TransportThrottleGroup.NO_THROTTLE;

public class ChunkedOutputTest
{
    private static final int DEFAULT_TEST_BUFFER_SIZE = 16;

    private final EmbeddedChannel channel = new EmbeddedChannel();
    private ChunkedOutput out;

    @BeforeEach
    void setUp()
    {
        out = new ChunkedOutput( channel, DEFAULT_TEST_BUFFER_SIZE, DEFAULT_TEST_BUFFER_SIZE, NO_THROTTLE );
    }

    @AfterEach
    void tearDown()
    {
        out.close();
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldFlushNothingWhenEmpty() throws Exception
    {
        out.flush();
        assertEquals( 0, channel.outboundMessages().size() );
    }

    @Test
    void shouldFlushNothingWhenClosed() throws Exception
    {
        out.close();
        out.flush();
        assertEquals( 0, channel.outboundMessages().size() );
    }

    @Test
    void shouldWriteAndFlushByte() throws Exception
    {
        out.beginMessage();
        out.writeByte( (byte) 42 );
        out.messageSucceeded();
        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, chunkContaining( (byte) 42 ) + messageBoundary() );
    }

    @Test
    void shouldWriteAndFlushShort() throws Exception
    {
        out.beginMessage();
        out.writeShort( (short) 42 );
        out.messageSucceeded();
        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, chunkContaining( (short) 42 ) + messageBoundary() );
    }

    @Test
    void shouldWriteAndFlushInt() throws Exception
    {
        out.beginMessage();
        out.writeInt( 424242 );
        out.messageSucceeded();
        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, chunkContaining( 424242 ) + messageBoundary() );
    }

    @Test
    void shouldWriteAndFlushLong() throws Exception
    {
        out.beginMessage();
        out.writeLong( 42424242 );
        out.messageSucceeded();
        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, chunkContaining( 42424242L ) + messageBoundary() );
    }

    @Test
    void shouldWriteAndFlushDouble() throws Exception
    {
        out.beginMessage();
        out.writeDouble( 42.4224 );
        out.messageSucceeded();
        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, chunkContaining( 42.4224 ) + messageBoundary() );
    }

    @Test
    void shouldWriteAndFlushByteBuffer() throws Exception
    {
        out.beginMessage();
        out.writeBytes( ByteBuffer.wrap( new byte[]{9, 8, 7, 6, 5, 4, 3, 2, 1} ) );
        out.messageSucceeded();
        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, chunkContaining( (byte) 9, (byte) 8, (byte) 7, (byte) 6, (byte) 5, (byte) 4, (byte) 3, (byte) 2, (byte) 1 ) +
                                             messageBoundary() );
    }

    @Test
    void shouldWriteAndFlushByteArray() throws Exception
    {
        out.beginMessage();
        out.writeBytes( new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, 1, 5 );
        out.messageSucceeded();
        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, chunkContaining( (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6 ) + messageBoundary() );
    }

    @Test
    void shouldThrowWhenByteArrayContainsInsufficientBytes() throws Exception
    {
        var e = assertThrows( IOException.class, () -> out.writeBytes( new byte[]{1, 2, 3}, 1, 5 ) );
        assertEquals( "Asked to write 5 bytes, but there is only 2 bytes available in data provided.", e.getMessage() );
    }

    @Test
    void shouldFlushOnClose() throws Exception
    {
        out.beginMessage();
        out.writeInt( 42 ).writeInt( 4242 ).writeInt( 424242 );
        out.messageSucceeded();
        out.close();

        ByteBuf outboundMessage = peekSingleOutboundMessage();
        assertByteBufEqual( outboundMessage, chunkContaining( 42, 4242, 424242 ) +
                                             messageBoundary() );
    }

    @Test
    void shouldCloseNothingWhenAlreadyClosed() throws Exception
    {
        out.beginMessage();
        out.writeLong( 42 );
        out.messageSucceeded();

        out.close();
        out.close();
        out.close();

        ByteBuf outboundMessage = peekSingleOutboundMessage();
        assertByteBufEqual( outboundMessage, chunkContaining( (long) 42 ) + messageBoundary() );
    }

    @Test
    void shouldChunkSingleMessage() throws Throwable
    {
        out.beginMessage();
        out.writeByte( (byte) 1 );
        out.writeShort( (short) 2 );
        out.messageSucceeded();
        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();
        assertByteBufEqual( outboundMessage, chunkContaining( (byte) 1, (short) 2 ) + messageBoundary() );
    }

    @Test
    void shouldChunkMessageSpanningMultipleChunks() throws Throwable
    {
        out.beginMessage();
        out.writeLong( 1 );
        out.writeLong( 2 );
        out.writeLong( 3 );
        out.messageSucceeded();
        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, chunkContaining( (long) 1 ) + chunkContaining( (long) 2 ) + chunkContaining( (long) 3 ) + messageBoundary() );
    }

    @Test
    void shouldChunkDataWhoseSizeIsGreaterThanOutputBufferCapacity() throws IOException
    {
        out.beginMessage();
        byte[] bytes = new byte[16];
        Arrays.fill( bytes, (byte) 42 );
        out.writeBytes( bytes, 0, 16 );
        out.messageSucceeded();
        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        Number[] chunk1Body = new Number[14];
        Arrays.fill( chunk1Body, (byte) 42 );

        Number[] chunk2Body = new Number[2];
        Arrays.fill( chunk2Body, (byte) 42 );

        assertByteBufEqual( outboundMessage, chunkContaining( chunk1Body ) + chunkContaining( chunk2Body ) + messageBoundary() );
    }

    @Test
    void shouldNotThrowIfOutOfSyncFlush() throws Throwable
    {
        out.beginMessage();
        out.writeLong( 1 );
        out.writeLong( 2 );
        out.writeLong( 3 );
        out.messageSucceeded();

        out.flush();
        out.close();
        //this flush comes in to late but should not cause ChunkedOutput to choke.
        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, chunkContaining( (long) 1 ) + chunkContaining( (long) 2 ) + chunkContaining( (long) 3 ) + messageBoundary() );
    }

    @Test
    void shouldNotBeAbleToWriteAfterClose() throws Throwable
    {
        out.beginMessage();
        out.writeLong( 1 );
        out.writeLong( 2 );
        out.writeLong( 3 );
        out.messageSucceeded();

        out.flush();
        out.close();

        assertThrows( IOException.class, () -> out.writeShort( (short) 42 ) );
    }

    @Test
    void shouldThrowErrorWithRemoteAddressWhenClosed() throws Exception
    {
        Channel channel = mock( Channel.class );
        ByteBufAllocator allocator = mock( ByteBufAllocator.class );
        when( allocator.buffer( anyInt() ) ).thenReturn( Unpooled.buffer() );
        when( channel.alloc() ).thenReturn( allocator );
        SocketAddress remoteAddress = mock( SocketAddress.class );
        String remoteAddressString = "client.server.com:7687";
        when( remoteAddress.toString() ).thenReturn( remoteAddressString );
        when( channel.remoteAddress() ).thenReturn( remoteAddress );

        ChunkedOutput output = new ChunkedOutput( channel, DEFAULT_TEST_BUFFER_SIZE, DEFAULT_TEST_BUFFER_SIZE, NO_THROTTLE );
        output.close();

        var e = assertThrows( PackOutputClosedException.class, () -> output.writeInt( 42 ) );
        assertThat( e.getMessage(), containsString( remoteAddressString ) );
    }

    @Test
    void shouldTruncateFailedMessage() throws Exception
    {
        out.beginMessage();
        out.writeInt( 1 );
        out.writeInt( 2 );
        out.messageSucceeded();

        out.beginMessage();
        out.writeInt( 3 );
        out.writeInt( 4 );
        out.messageFailed();

        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, chunkContaining( 1, 2 ) + messageBoundary() );
    }

    @Test
    void shouldAllowWritingAfterFailedMessage() throws Exception
    {
        out.beginMessage();
        out.writeInt( 1 );
        out.writeInt( 2 );
        out.messageSucceeded();

        out.beginMessage();
        out.writeByte( (byte) 3 );
        out.writeByte( (byte) 4 );
        out.messageFailed();

        out.beginMessage();
        out.writeInt( 33 );
        out.writeLong( 44 );
        out.messageSucceeded();

        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, chunkContaining( 1, 2 ) + messageBoundary() +
                                             chunkContaining( 33, (long) 44 ) + messageBoundary() );
    }

    @Test
    void shouldWriteOnlyMessageBoundaryWhenWriterIsEmpty() throws Exception
    {
        out.beginMessage();
        // write nothing in the message body
        out.messageSucceeded();
        out.flush();

        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, messageBoundary() );
    }

    @Test
    void shouldAutoFlushOnlyWhenMaxBufferSizeReachedAfterFullMessage() throws Exception
    {
        out.beginMessage();
        out.writeInt( 1 );
        out.writeInt( 2 );
        out.writeInt( 3 );
        out.writeLong( 4 );

        // nothing should be flushed because we are still in the middle of the message
        assertEquals( 0, peekAllOutboundMessages().size() );

        out.writeByte( (byte) 5 );
        out.writeByte( (byte) 6 );
        out.writeLong( 7 );
        out.writeInt( 8 );
        out.writeByte( (byte) 9 );

        // still nothing should be flushed
        assertEquals( 0, peekAllOutboundMessages().size() );

        out.messageSucceeded();

        // now the whole buffer should be flushed, it is larger than the maxBufferSize
        ByteBuf outboundMessage = peekSingleOutboundMessage();

        assertByteBufEqual( outboundMessage, chunkContaining( 1, 2, 3 ) +
                                             chunkContaining( (long) 4, (byte) 5, (byte) 6 ) +
                                             chunkContaining( (long) 7, 8, (byte) 9 ) +
                                             messageBoundary() );
    }

    @Test
    void shouldAutoFlushMultipleMessages() throws Exception
    {
        out.beginMessage();
        out.writeLong( 1 );
        out.writeLong( 2 );
        out.messageSucceeded();

        out.beginMessage();
        out.writeLong( 3 );
        out.writeLong( 4 );
        out.messageSucceeded();

        out.beginMessage();
        out.writeLong( 5 );
        out.writeLong( 6 );
        out.messageSucceeded();

        List<ByteBuf> outboundMessages = peekAllOutboundMessages();
        assertEquals( 3, outboundMessages.size() );

        assertByteBufEqual( outboundMessages.get( 0 ), chunkContaining( (long) 1 ) + chunkContaining( (long) 2 ) + messageBoundary() );
        assertByteBufEqual( outboundMessages.get( 1 ), chunkContaining( (long) 3 ) + chunkContaining( (long) 4 ) + messageBoundary() );
        assertByteBufEqual( outboundMessages.get( 2 ), chunkContaining( (long) 5 ) + chunkContaining( (long) 6 ) + messageBoundary() );
    }

    @Test
    void shouldFailToBeginMultipleMessages()
    {
        out.beginMessage();

        assertThrows( IllegalStateException.class, () -> out.beginMessage() );
    }

    @Test
    void shouldFailToMarkMessageAsSuccessfulWhenMessageNotStarted() throws Exception
    {
        assertThrows( IllegalStateException.class, () -> out.messageSucceeded() );
    }

    @Test
    void shouldFailToMarkMessageAsFialedWhenMessageNotStarted() throws Exception
    {
        assertThrows( IllegalStateException.class, () -> out.messageFailed() );
    }

    @Test
    void shouldFailToWriteByteOutsideOfMessage() throws Exception
    {
        assertThrows( IllegalStateException.class, () -> out.writeByte( (byte) 1 ) );
    }

    @Test
    void shouldFailToWriteShortOutsideOfMessage() throws Exception
    {
        assertThrows( IllegalStateException.class, () -> out.writeShort( (short) 1 ) );
    }

    @Test
    void shouldFailToWriteIntOutsideOfMessage() throws Exception
    {
        assertThrows( IllegalStateException.class, () -> out.writeInt( 1 ) );
    }

    @Test
    void shouldFailToWriteLongOutsideOfMessage() throws Exception
    {
        assertThrows( IllegalStateException.class, () -> out.writeLong( 1 ) );
    }

    @Test
    void shouldFailToWriteDoubleOutsideOfMessage() throws Exception
    {
        assertThrows( IllegalStateException.class, () -> out.writeDouble( 1.1 ) );
    }

    @Test
    void shouldFailToWriteBytesOutsideOfMessage() throws Exception
    {
        assertThrows( IllegalStateException.class, () -> out.writeBytes( ByteBuffer.wrap( new byte[10] ) ) );
    }

    @Test
    void shouldFailToMarkMessageAsSuccessfulAndThenAsFailed() throws Exception
    {
        out.beginMessage();
        out.writeInt( 42 );
        out.messageSucceeded();

        assertThrows( IllegalStateException.class, () -> out.messageFailed() );

        out.flush();

        assertByteBufEqual( peekSingleOutboundMessage(), chunkContaining( 42 ) + messageBoundary() );
    }

    @Test
    void shouldFailToMarkMessageAsFailedAndThenAsSuccessful() throws Exception
    {
        out.beginMessage();
        out.writeInt( 42 );
        out.messageFailed();

        assertThrows( IllegalStateException.class, () -> out.messageSucceeded() );

        out.flush();

        assertEquals( 0, peekAllOutboundMessages().size() );
    }

    @Test
    void shouldAllowMultipleFailedMessages() throws Exception
    {
        for ( int i = 0; i < 7; i++ )
        {
            out.beginMessage();
            out.writeByte( (byte) i );
            out.writeShort( (short) i );
            out.writeInt( i );
            out.messageFailed();
        }

        out.flush();
        assertEquals( 0, peekAllOutboundMessages().size() );

        // try to write a 2-chunk message which should auto-flush
        out.beginMessage();
        out.writeByte( (byte) 8 );
        out.writeShort( (short) 9 );
        out.writeInt( 10 );
        out.writeDouble( 199.92 );
        out.messageSucceeded();

        assertByteBufEqual( peekSingleOutboundMessage(), chunkContaining( (byte) 8, (short) 9, 10 ) +
                                                         chunkContaining( 199.92 ) +
                                                         messageBoundary() );
    }

    private ByteBuf peekSingleOutboundMessage()
    {
        List<ByteBuf> outboundMessages = peekAllOutboundMessages();
        assertEquals( 1, outboundMessages.size() );
        return outboundMessages.get( 0 );
    }

    private List<ByteBuf> peekAllOutboundMessages()
    {
        return channel.outboundMessages()
                .stream()
                .map( msg -> (ByteBuf) msg )
                .collect( toList() );
    }

    private static void assertByteBufEqual( ByteBuf buf, String hexContent )
    {
        assertEquals( ByteBufUtil.hexDump( buf ), hexContent );
    }

    private static String chunkContaining( Number... values )
    {
        short chunkSize = 0;
        for ( Number value : values )
        {
            if ( value instanceof Byte )
            {
                chunkSize += Byte.BYTES;
            }
            else if ( value instanceof Short )
            {
                chunkSize += Short.BYTES;
            }
            else if ( value instanceof Integer )
            {
                chunkSize += Integer.BYTES;
            }
            else if ( value instanceof Long )
            {
                chunkSize += Long.BYTES;
            }
            else if ( value instanceof Double )
            {
                chunkSize += Double.BYTES;
            }
            else
            {
                throw new IllegalArgumentException( "Unsupported number " + value.getClass() + ' ' + value );
            }
        }

        ByteBuffer buffer = ByteBuffers.allocate( chunkSize + CHUNK_HEADER_SIZE );
        buffer.putShort( chunkSize );

        for ( Number value : values )
        {
            if ( value instanceof Byte )
            {
                buffer.put( value.byteValue() );
            }
            else if ( value instanceof Short )
            {
                buffer.putShort( value.shortValue() );
            }
            else if ( value instanceof Integer )
            {
                buffer.putInt( value.intValue() );
            }
            else if ( value instanceof Long )
            {
                buffer.putLong( value.longValue() );
            }
            else if ( value instanceof Double )
            {
                buffer.putDouble( value.doubleValue() );
            }
            else
            {
                throw new IllegalArgumentException( "Unsupported number " + value.getClass() + ' ' + value );
            }
        }
        buffer.flip();

        return ByteBufUtil.hexDump( buffer.array() );
    }

    private static String messageBoundary()
    {
        ByteBuffer buffer = ByteBuffers.allocate( Short.BYTES );
        buffer.putShort( (short) 0 );
        buffer.flip();
        return ByteBufUtil.hexDump( buffer.array() );
    }
}
