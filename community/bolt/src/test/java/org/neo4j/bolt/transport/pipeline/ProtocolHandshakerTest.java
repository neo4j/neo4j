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
package org.neo4j.bolt.transport.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.NoSuchElementException;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.logging.BoltMessageLogger;
import org.neo4j.bolt.logging.NullBoltMessageLogger;
import org.neo4j.bolt.transport.BoltProtocolPipelineInstaller;
import org.neo4j.bolt.transport.BoltProtocolPipelineInstallerFactory;
import org.neo4j.logging.AssertableLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.BoltTestUtil.assertByteBufEquals;

public class ProtocolHandshakerTest
{
    private final String connector = "default";
    private final Channel channel = mock( Channel.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final BoltMessageLogger messageLogger = NullBoltMessageLogger.getInstance();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void shouldChooseFirstAvailableProtocol()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, channel, messageLogger ) )
        {
            // Given
            BoltProtocolPipelineInstaller handler = newHandler( 1 );
            BoltProtocolPipelineInstallerFactory handlerFactory = newHandlerFactory( 1, handler );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

            // When
            ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                    new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                    new byte[]{0, 0, 0, 0}, // first choice - no protocol
                    new byte[]{0, 0, 0, 1}, // second choise - protocol 1
                    new byte[]{0, 0, 0, 0}, // third choice - no protocol
                    new byte[]{0, 0, 0, 0} ); // fourth choice - no protocol
            channel.writeInbound( input );

            // Then
            assertEquals( 1, channel.outboundMessages().size() );
            assertByteBufEquals( Unpooled.buffer().writeInt( 1 ), channel.readOutbound() );

            thrown.expect( NoSuchElementException.class );
            channel.pipeline().remove( ProtocolHandshaker.class );

            assertTrue( channel.isActive() );
            verify( handler ).install();
        }
    }

    @Test
    public void shouldHandleFragmentedMessage()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, channel, messageLogger ) )
        {
            // Given
            BoltProtocolPipelineInstaller handler = newHandler( 1 );
            BoltProtocolPipelineInstallerFactory handlerFactory = newHandlerFactory( 1, handler );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

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

            thrown.expect( NoSuchElementException.class );
            channel.pipeline().remove( ProtocolHandshaker.class );

            assertTrue( channel.isActive() );
            verify( handler ).install();
        }
    }

    @Test
    public void shouldHandleHandshakeFollowedImmediatelyByMessage()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, channel, messageLogger ) )
        {
            // Given
            BoltProtocolPipelineInstaller handler = newHandler( 1 );
            BoltProtocolPipelineInstallerFactory handlerFactory = newHandlerFactory( 1, handler );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

            // When
            ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                    new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                    new byte[]{0, 0, 0, 0}, // first choice - no protocol
                    new byte[]{0, 0, 0, 1}, // second choise - protocol 1
                    new byte[]{0, 0, 0, 0}, // third choice - no protocol
                    new byte[]{0, 0, 0, 0}, // fourth choice - no protocol
                    new byte[]{1, 2, 3, 4} ); // this is a message
            channel.writeInbound( input );

            // Then
            assertEquals( 1, channel.outboundMessages().size() );
            assertByteBufEquals( Unpooled.buffer().writeInt( 1 ), channel.readOutbound() );

            assertEquals( 1, channel.inboundMessages().size() );
            assertByteBufEquals( Unpooled.wrappedBuffer( new byte[]{1, 2, 3, 4} ), channel.readInbound() );

            thrown.expect( NoSuchElementException.class );
            channel.pipeline().remove( ProtocolHandshaker.class );

            assertTrue( channel.isActive() );
            verify( handler ).install();
        }
    }

    @Test
    public void shouldHandleMaxVersionNumber()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, channel, messageLogger ) )
        {
            long maxVersionNumber = 4_294_967_295L;

            // Given
            BoltProtocolPipelineInstaller handler = newHandler( maxVersionNumber );
            BoltProtocolPipelineInstallerFactory handlerFactory = newHandlerFactory( maxVersionNumber, handler );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

            // When
            ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                    new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                    new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, // first choice - no protocol
                    new byte[]{0, 0, 0, 0}, // second choise - protocol 1
                    new byte[]{0, 0, 0, 0}, // third choice - no protocol
                    new byte[]{0, 0, 0, 0} ); // fourth choice - no protocol
            channel.writeInbound( input );

            // Then
            assertEquals( 1, channel.outboundMessages().size() );
            assertByteBufEquals( Unpooled.buffer().writeInt( (int)maxVersionNumber ), channel.readOutbound() );

            thrown.expect( NoSuchElementException.class );
            channel.pipeline().remove( ProtocolHandshaker.class );

            assertTrue( channel.isActive() );
            verify( handler ).install();
        }
    }

    @Test
    public void shouldFallbackToNoProtocolIfNoMatch()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, channel, messageLogger ) )
        {
            // Given
            BoltProtocolPipelineInstaller handler = newHandler( 1 );
            BoltProtocolPipelineInstallerFactory handlerFactory = newHandlerFactory( 1, handler );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

            // When
            ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                    new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                    new byte[]{0, 0, 0, 0}, // first choice - no protocol
                    new byte[]{0, 0, 0, 2}, // second choise - protocol 1
                    new byte[]{0, 0, 0, 3}, // third choice - no protocol
                    new byte[]{0, 0, 0, 4} ); // fourth choice - no protocol
            channel.writeInbound( input );

            // Then
            assertEquals( 1, channel.outboundMessages().size() );
            assertByteBufEquals( Unpooled.buffer().writeInt( 0 ), channel.readOutbound() );

            assertFalse( channel.isActive() );
            verify( handler, never() ).install();
        }
    }

    @Test
    public void shouldRejectIfWrongPreamble()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, channel, messageLogger ) )
        {
            // Given
            BoltProtocolPipelineInstaller handler = newHandler( 1 );
            BoltProtocolPipelineInstallerFactory handlerFactory = newHandlerFactory( 1, handler );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

            // When
            ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                    new byte[]{(byte) 0xDE, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF}, // preamble
                    new byte[]{0, 0, 0, 1}, // first choice - no protocol
                    new byte[]{0, 0, 0, 2}, // second choise - protocol 1
                    new byte[]{0, 0, 0, 3}, // third choice - no protocol
                    new byte[]{0, 0, 0, 4} ); // fourth choice - no protocol
            channel.writeInbound( input );

            // Then
            assertEquals( 0, channel.outboundMessages().size() );
            assertFalse( channel.isActive() );
            verify( handler, never() ).install();
        }
    }

    @Test
    public void shouldRejectIfInsecureWhenEncryptionRequired()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, channel, messageLogger ) )
        {
            // Given
            BoltProtocolPipelineInstaller handler = newHandler( 1 );
            BoltProtocolPipelineInstallerFactory handlerFactory = newHandlerFactory( 1, handler );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, true, false ) );

            // When
            ByteBuf input = Unpooled.wrappedBuffer( // create handshake data
                    new byte[]{(byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17}, // preamble
                    new byte[]{0, 0, 0, 1}, // first choice - no protocol
                    new byte[]{0, 0, 0, 2}, // second choise - protocol 1
                    new byte[]{0, 0, 0, 3}, // third choice - no protocol
                    new byte[]{0, 0, 0, 4} ); // fourth choice - no protocol
            channel.writeInbound( input );

            // Then
            assertEquals( 0, channel.outboundMessages().size() );
            assertFalse( channel.isActive() );
            verify( handler, never() ).install();
        }
    }

    @Test
    public void shouldRejectIfHttp()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, channel, messageLogger ) )
        {
            // Given
            BoltProtocolPipelineInstaller handler = newHandler( 1 );
            BoltProtocolPipelineInstallerFactory handlerFactory = newHandlerFactory( 1, handler );
            EmbeddedChannel channel = new EmbeddedChannel( new ProtocolHandshaker( handlerFactory, boltChannel, logProvider, false, true ) );

            // When
            FullHttpRequest request = new DefaultFullHttpRequest( HttpVersion.HTTP_1_1, HttpMethod.POST, "http://hello_world:10000" );
            request.headers().setInt( HttpHeaderNames.CONTENT_LENGTH, 0 );
            channel.writeInbound( request );

            // Then
            assertEquals( 0, channel.outboundMessages().size() );
            assertFalse( channel.isActive() );
            verify( handler, never() ).install();
            logProvider.assertExactly( AssertableLogProvider.inLog( ProtocolHandshaker.class ).warn(
                    "Unsupported connection type: 'HTTP'. Bolt protocol only operates over a TCP connection or WebSocket." ) );
        }
    }

    private static BoltProtocolPipelineInstaller newHandler( long version )
    {
        BoltProtocolPipelineInstaller handler = mock( BoltProtocolPipelineInstaller.class );

        when( handler.version() ).thenReturn( version );

        return handler;
    }

    private static BoltProtocolPipelineInstallerFactory newHandlerFactory( long version, BoltProtocolPipelineInstaller handler )
    {
        return ( givenVersion, channel ) -> version == givenVersion ? handler : null;
    }
}
