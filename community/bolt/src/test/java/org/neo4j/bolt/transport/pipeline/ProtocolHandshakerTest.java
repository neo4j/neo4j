/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.transport.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.NoSuchElementException;
import java.util.stream.Stream;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltProtocol;
import org.neo4j.bolt.BoltProtocolVersion;
import org.neo4j.bolt.transport.BoltProtocolFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.memory.MemoryTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.BoltTestUtil.assertByteBufEquals;
import static org.neo4j.bolt.testing.BoltTestUtil.newTestBoltChannel;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;

class ProtocolHandshakerTest
{
    private final BoltChannel boltChannel = newTestBoltChannel();
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    private static Stream<Arguments> protocolVersionProviderInRange()
    {
        return Stream.of(Arguments.of(4, 0),
                         Arguments.of(4, 3),
                         Arguments.of(4, 2),
                         Arguments.of(4, 1));
    }

    private static Stream<Arguments> protocolVersionProviderOutOfRange()
    {
        return Stream.of(Arguments.of(4, 1),    //below lowest minor range
                         Arguments.of(3, 5),    //lower major
                         Arguments.of(5, 0),    //higher major
                         Arguments.of(4, 5));   //above highest minor and range
    }

    @AfterEach
    void tearDown()
    {
        boltChannel.close();
    }

    @Test
    void shouldChooseFirstAvailableProtocol()
    {
        // Given
        BoltProtocol protocol = newBoltProtocol( 1, 0 );
        BoltProtocolFactory handlerFactory = newProtocolFactory( 1, 0, protocol );
        var memoryTracker = mock( MemoryTracker.class, RETURNS_MOCKS );

        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true, mock( ChannelProtector.class ), memoryTracker ) );

        // When
        ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                                                new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                                                new byte[]{0, 0, 0, 0}, // first choice - no protocol
                                                new byte[]{0, 0, 0, 1}, // second choice - protocol 1
                                                new byte[]{0, 0, 0, 0}, // third choice - no protocol
                                                new byte[]{0, 0, 0, 0} ); // fourth choice - no protocol
        channel.writeInbound( input );

        // Then
        assertEquals( 1, channel.outboundMessages().size() );
        assertByteBufEquals( Unpooled.buffer().writeInt( 1 ), channel.readOutbound() );

        assertThrows( NoSuchElementException.class, () -> channel.pipeline().remove( ProtocolHandshaker.class ) );

        assertTrue( channel.isActive() );
        verify( protocol ).install();
    }

    @ParameterizedTest
    @MethodSource( "protocolVersionProviderInRange" )
    void shouldHandleProtocolRanges( int major, int minor )
    {
        //Given
        int packedVersion = new BoltProtocolVersion( major, minor ).toInt();
        BoltProtocol protocol = newBoltProtocol( major, minor );
        BoltProtocolFactory handlerFactory = newProtocolFactory( major, minor, protocol );
        var memoryTracker = mock( MemoryTracker.class, RETURNS_MOCKS );

        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true, mock( ChannelProtector.class ), memoryTracker ) );

        // When
        ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                                                new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                                                new byte[]{0, 0, 0, 4},  // first choice - 4.0 no range
                                                new byte[]{0, 2, 3, 4},  // second choice - 4.3 -> 4.1 inclusive range of 2
                                                new byte[]{0, 0, 0, 0},  // No protocol
                                                new byte[]{0, 0, 0, 0} ); // No protocol
        channel.writeInbound( input );

        //Then
        assertEquals( 1, channel.outboundMessages().size() );
        assertByteBufEquals( Unpooled.buffer().writeInt( packedVersion ), channel.readOutbound() );

        assertThrows( NoSuchElementException.class, () -> channel.pipeline().remove( ProtocolHandshaker.class ) );
        assertTrue( channel.isActive() );
    }

    @ParameterizedTest
    @MethodSource( "protocolVersionProviderOutOfRange" )
    void shouldFailOutOfRangeProtocol( int major, int minor )
    {
        //Given
        int noVersion = 0;
        BoltProtocol protocol = newBoltProtocol( major, minor );
        BoltProtocolFactory handlerFactory = newProtocolFactory( major, minor, protocol );

        var memoryTracker = mock( MemoryTracker.class );
        var scopedTracker = mock( MemoryTracker.class );
        when( memoryTracker.getScopedMemoryTracker() ).thenReturn( scopedTracker );

        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true, mock( ChannelProtector.class ), memoryTracker ) );

        // When
        ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                                                new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                                                new byte[]{0, 0, 0, 4},  // first choice - 4.0 no range
                                                new byte[]{0, 2, 4, 4},  // second choice - 4.4 -> 4.2 inclusive range of 2
                                                new byte[]{0, 0, 0, 0},  // No protocol
                                                new byte[]{0, 0, 0, 0} ); // No protocol
        channel.writeInbound( input );

        //Then
        assertEquals( 1, channel.outboundMessages().size() );
        assertByteBufEquals( Unpooled.buffer().writeInt( noVersion ), channel.readOutbound() );

        assertThrows( NoSuchElementException.class, () -> channel.pipeline().remove( ProtocolHandshaker.class ) );
        assertFalse( channel.isActive() );
    }

    @Test
    void shouldHandleFragmentedMessage()
    {
        // Given
        BoltProtocol protocol = newBoltProtocol( 1, 0 );
        BoltProtocolFactory handlerFactory = newProtocolFactory( 1, 0, protocol );
        var memoryTracker = mock( MemoryTracker.class, RETURNS_MOCKS );

        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true, mock( ChannelProtector.class ), memoryTracker ) );

        // When
        channel.writeInbound( Unpooled.wrappedBuffer( new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0} ) );
        assertEquals( 0, channel.outboundMessages().size() );
        channel.writeInbound( Unpooled.wrappedBuffer( new byte[]{(byte) 0x17, 0, 0, 0} ) );
        assertEquals( 0, channel.outboundMessages().size() );
        channel.writeInbound( Unpooled.wrappedBuffer( new byte[]{0, 0, 0} ) );
        assertEquals( 0, channel.outboundMessages().size() );
        channel.writeInbound( Unpooled.wrappedBuffer( new byte[]{0, 1, 0, 0, 0} ) );
        assertEquals( 0, channel.outboundMessages().size() );
        channel.writeInbound( Unpooled.wrappedBuffer( new byte[]{0, 0, 0} ) );
        assertEquals( 0, channel.outboundMessages().size() );
        channel.writeInbound( Unpooled.wrappedBuffer( new byte[]{0, 0} ) );

        // Then
        assertEquals( 1, channel.outboundMessages().size() );
        assertByteBufEquals( Unpooled.buffer().writeInt( 1 ), channel.readOutbound() );

        assertThrows( NoSuchElementException.class, () -> channel.pipeline().remove( ProtocolHandshaker.class ) );

        assertTrue( channel.isActive() );
        verify( protocol ).install();
    }

    @Test
    void shouldHandleHandshakeFollowedImmediatelyByMessage()
    {
        // Given
        BoltProtocol protocol = newBoltProtocol( 1, 0 );
        BoltProtocolFactory handlerFactory = newProtocolFactory( 1, 0, protocol );
        var memoryTracker = mock( MemoryTracker.class, RETURNS_MOCKS );

        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true, mock( ChannelProtector.class ), memoryTracker ) );

        // When
        ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                                                new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                                                new byte[]{0, 0, 0, 0}, // first choice - no protocol
                                                new byte[]{0, 0, 0, 1}, // second choice - protocol 1
                                                new byte[]{0, 0, 0, 0}, // third choice - no protocol
                                                new byte[]{0, 0, 0, 0}, // fourth choice - no protocol
                                                new byte[]{1, 2, 3, 4} ); // this is a message
        channel.writeInbound( input );

        // Then
        assertEquals( 1, channel.outboundMessages().size() );
        assertByteBufEquals( Unpooled.buffer().writeInt( 1 ), channel.readOutbound() );

        assertEquals( 1, channel.inboundMessages().size() );
        assertByteBufEquals( Unpooled.wrappedBuffer( new byte[]{1, 2, 3, 4} ), channel.readInbound() );

        assertThrows( NoSuchElementException.class, () -> channel.pipeline().remove( ProtocolHandshaker.class ) );

        assertTrue( channel.isActive() );
        verify( protocol ).install();
    }

    @Test
    void shouldHandleMaxVersionNumber()
    {
        int maxVersionNumber = 255;

        // Given
        BoltProtocol protocol = newBoltProtocol( maxVersionNumber, maxVersionNumber );
        BoltProtocolFactory handlerFactory = newProtocolFactory( maxVersionNumber, maxVersionNumber, protocol );
        var memoryTracker = mock( MemoryTracker.class, RETURNS_MOCKS );

        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true, mock( ChannelProtector.class ), memoryTracker ) );

        // When
        ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                                                new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                                                new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, // first choice - no protocol
                                                new byte[]{0, 0, 0, 0}, // second choice - protocol 1
                                                new byte[]{0, 0, 0, 0}, // third choice - no protocol
                                                new byte[]{0, 0, 0, 0} ); // fourth choice - no protocol
        channel.writeInbound( input );

        // Then
        assertEquals( 1, channel.outboundMessages().size() );
        assertByteBufEquals( Unpooled.buffer()
                                     .writeInt( new BoltProtocolVersion( maxVersionNumber, maxVersionNumber ).toInt() ), channel.readOutbound() );

        assertThrows( NoSuchElementException.class, () -> channel.pipeline().remove( ProtocolHandshaker.class ) );

        assertTrue( channel.isActive() );
        verify( protocol ).install();
    }

    @Test
    void shouldFallbackToNoProtocolIfNoMatch()
    {
        // Given
        BoltProtocol protocol = newBoltProtocol( 1, 0 );
        BoltProtocolFactory handlerFactory = newProtocolFactory( 1, 0, protocol );
        var memoryTracker = mock( MemoryTracker.class, RETURNS_MOCKS );

        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true, mock( ChannelProtector.class ), memoryTracker ) );

        // When
        ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                                                new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                                                new byte[]{0, 0, 0, 0}, // first choice - no protocol
                                                new byte[]{0, 0, 0, 2}, // second choice - protocol 1
                                                new byte[]{0, 0, 0, 3}, // third choice - no protocol
                                                new byte[]{0, 0, 0, 4} ); // fourth choice - no protocol
        channel.writeInbound( input );

        // Then
        assertEquals( 1, channel.outboundMessages().size() );
        assertByteBufEquals( Unpooled.buffer().writeInt( 0 ), channel.readOutbound() );

        assertFalse( channel.isActive() );
        verify( protocol, never() ).install();
    }

    @Test
    void shouldRejectIfWrongPreamble()
    {
        // Given
        BoltProtocol protocol = newBoltProtocol( 1, 0 );
        BoltProtocolFactory handlerFactory = newProtocolFactory( 1, 0, protocol );
        var memoryTracker = mock( MemoryTracker.class );

        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true, mock( ChannelProtector.class ), memoryTracker ) );

        // When
        ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                                                new byte[]{(byte) 0xDE, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF}, // preamble
                                                new byte[]{0, 0, 0, 1}, // first choice - no protocol
                                                new byte[]{0, 0, 0, 2}, // second choice - protocol 1
                                                new byte[]{0, 0, 0, 3}, // third choice - no protocol
                                                new byte[]{0, 0, 0, 4} ); // fourth choice - no protocol
        channel.writeInbound( input );

        // Then
        assertEquals( 0, channel.outboundMessages().size() );
        assertFalse( channel.isActive() );
        verify( protocol, never() ).install();
    }

    @Test
    void shouldRejectIfInsecureWhenEncryptionRequired()
    {
        // Given
        BoltProtocol protocol = newBoltProtocol( 1, 0 );
        BoltProtocolFactory handlerFactory = newProtocolFactory( 1, 0, protocol );
        var memoryTracker = mock( MemoryTracker.class );

        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, true, false, mock( ChannelProtector.class ), memoryTracker ) );

        // When
        ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                                                new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                                                new byte[]{0, 0, 0, 1}, // first choice - no protocol
                                                new byte[]{0, 0, 0, 2}, // second choice - protocol 1
                                                new byte[]{0, 0, 0, 3}, // third choice - no protocol
                                                new byte[]{0, 0, 0, 4} ); // fourth choice - no protocol
        channel.writeInbound( input );

        // Then
        assertEquals( 0, channel.outboundMessages().size() );
        assertFalse( channel.isActive() );
        verify( protocol, never() ).install();
    }

    @Test
    void shouldRejectIfHttp()
    {
        // Given
        BoltProtocol protocol = newBoltProtocol( 1, 0 );
        BoltProtocolFactory handlerFactory = newProtocolFactory( 1, 0, protocol );
        var memoryTracker = mock( MemoryTracker.class );

        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true, mock( ChannelProtector.class ), memoryTracker ) );

        // When
        FullHttpRequest request = new DefaultFullHttpRequest( HttpVersion.HTTP_1_1, HttpMethod.POST, "http://hello_world:10000" );
        request.headers().setInt( HttpHeaderNames.CONTENT_LENGTH, 0 );
        channel.writeInbound( request );

        // Then
        assertEquals( 0, channel.outboundMessages().size() );
        assertFalse( channel.isActive() );
        verify( protocol, never() ).install();
        assertThat( logProvider ).forClass( ProtocolHandshaker.class ).forLevel( WARN ).containsMessages(
                "Unsupported connection type: 'HTTP'. Bolt protocol only operates over a TCP connection or WebSocket." );
    }

    @Test
    void shouldAllocateUponNegotiation()
    {
        var protocol = newBoltProtocol( 1, 0 );
        var handlerFactory = newProtocolFactory( 1, 0, protocol );
        var memoryTracker = mock( MemoryTracker.class );
        var versionMemoryTracker = mock( MemoryTracker.class );

        when( memoryTracker.getScopedMemoryTracker() )
                .thenReturn( versionMemoryTracker );

        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true, mock( ChannelProtector.class ), memoryTracker ) );

        var payload = Unpooled.wrappedBuffer(
                new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17},
                new byte[]{0, 0, 0, 2},
                new byte[]{0, 0, 5, 3},
                new byte[]{0, 8, 4, 8},
                new byte[]{0, 0, 5, 0}
        );
        channel.writeInbound( payload );

        var inOrder = inOrder( memoryTracker, versionMemoryTracker );
        inOrder.verify( memoryTracker ).getScopedMemoryTracker();

        inOrder.verify( versionMemoryTracker, times( 2 ) ).allocateHeap( BoltProtocolVersion.SHALLOW_SIZE );
        inOrder.verify( versionMemoryTracker ).allocateHeap( BoltProtocolVersion.SHALLOW_SIZE * 9 );
        inOrder.verify( versionMemoryTracker ).allocateHeap( BoltProtocolVersion.SHALLOW_SIZE );
        inOrder.verify( versionMemoryTracker ).close();
        inOrder.verify( memoryTracker ).releaseHeap( ProtocolHandshaker.SHALLOW_SIZE );
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldFreeMemoryUponRemoval()
    {
        var protocol = newBoltProtocol( 1, 0 );
        var handlerFactory = newProtocolFactory( 1, 0, protocol );
        var memoryTracker = mock( MemoryTracker.class );

        EmbeddedChannel channel = new EmbeddedChannel(
                new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true, mock( ChannelProtector.class ), memoryTracker ) );

        channel.pipeline()
               .removeFirst();

        verify( memoryTracker ).releaseHeap( ProtocolHandshaker.SHALLOW_SIZE );
        verifyNoMoreInteractions( memoryTracker );
    }

    private static BoltProtocol newBoltProtocol( int majorVersion, int minorVersion )
    {
        BoltProtocol handler = mock( BoltProtocol.class );

        when( handler.version() ).thenReturn( new BoltProtocolVersion( majorVersion, minorVersion ) );

        return handler;
    }

    private static BoltProtocolFactory newProtocolFactory( int majorVersion, int minorVersion, BoltProtocol protocol )
    {
        BoltProtocolVersion version = new BoltProtocolVersion( majorVersion, minorVersion );
        return ( givenVersion, channel, channelProtector, memoryTracker ) -> version.equals( givenVersion ) ? protocol : null;
    }
}
