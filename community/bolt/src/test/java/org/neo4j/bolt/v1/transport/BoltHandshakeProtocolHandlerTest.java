/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.bolt.v1.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.logging.BoltMessageLogger;
import org.neo4j.bolt.logging.NullBoltMessageLogger;
import org.neo4j.bolt.transport.BoltHandshakeProtocolHandler;
import org.neo4j.bolt.transport.BoltMessagingProtocolHandler;
import org.neo4j.bolt.transport.BoltProtocolHandlerFactory;
import org.neo4j.bolt.transport.HandshakeOutcome;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.transport.HandshakeOutcome.INSECURE_HANDSHAKE;
import static org.neo4j.bolt.transport.HandshakeOutcome.INVALID_HANDSHAKE;
import static org.neo4j.bolt.transport.HandshakeOutcome.NO_APPLICABLE_PROTOCOL;
import static org.neo4j.bolt.transport.HandshakeOutcome.PARTIAL_HANDSHAKE;
import static org.neo4j.bolt.transport.HandshakeOutcome.PROTOCOL_CHOSEN;

public class BoltHandshakeProtocolHandlerTest
{
    private final String connector = "default";
    private final BoltMessagingProtocolHandler protocol = mock( BoltMessagingProtocolHandler.class );
    private final ChannelHandlerContext ctx = newChannelHandlerContextMock();
    private final BoltMessageLogger messageLogger = NullBoltMessageLogger.getInstance();

    @Test
    public void shouldChooseFirstAvailableProtocol()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, ctx, messageLogger ) )
        {
            // Given
            BoltProtocolHandlerFactory handlerFactory = newHandlerFactory( 1, protocol );
            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( handlerFactory, false, true );

            // When
            HandshakeOutcome outcome =
                    handshake.perform( boltChannel, wrappedBuffer( new byte[]{
                            (byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17,
                            0, 0, 0, 0,
                            0, 0, 0, 1,
                            0, 0, 0, 0,
                            0, 0, 0, 0 } ) );

            // Then
            assertThat( outcome, equalTo( PROTOCOL_CHOSEN ) );
            assertThat( handshake.chosenProtocol(), equalTo( protocol ) );
        }
    }

    @Test
    public void shouldHandleFragmentedMessage()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, ctx, messageLogger ) )
        {
            // Given
            BoltProtocolHandlerFactory handlerFactory = newHandlerFactory( 1, protocol );
            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( handlerFactory, false, true );

            // When
            HandshakeOutcome firstOutcome = handshake.perform( boltChannel, wrappedBuffer( new byte[]{
                    (byte) 0x60, (byte) 0x60 } ) );
            // When
            HandshakeOutcome secondOutcome = handshake.perform( boltChannel, wrappedBuffer( new byte[]{
                    (byte) 0xB0, (byte) 0x17 } ) );
            HandshakeOutcome thirdOutcome = handshake.perform( boltChannel, wrappedBuffer( new byte[]{
                    0, 0, 0, 0,
                    0, 0, 0 } ) );
            HandshakeOutcome fourthOutcome = handshake.perform( boltChannel, wrappedBuffer( new byte[]{
                    1,
                    0, 0, 0, 0,
                    0, 0, 0, 0 } ) );

            // Then
            assertThat( firstOutcome, equalTo( PARTIAL_HANDSHAKE ) );
            assertThat( secondOutcome, equalTo( PARTIAL_HANDSHAKE ) );
            assertThat( thirdOutcome, equalTo( PARTIAL_HANDSHAKE ) );
            assertThat( fourthOutcome, equalTo( PROTOCOL_CHOSEN ) );
            assertThat( handshake.chosenProtocol(), equalTo( protocol ) );
        }
    }

    @Test
    public void shouldHandleHandshakeFollowedByMessageInSameBuffer()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, ctx, messageLogger ) )
        {
            // Given
            BoltProtocolHandlerFactory handlerFactory = newHandlerFactory( 1, protocol );
            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( handlerFactory, false, true );

            // When
            ByteBuf buffer = wrappedBuffer( new byte[]{
                    (byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17,
                    0, 0, 0, 0,
                    0, 0, 0, 1,
                    0, 0, 0, 0,
                    0, 0, 0, 0,
                    1, 2, 3, 4 } ); // These last four bytes are not part of the handshake

            HandshakeOutcome outcome = handshake.perform( boltChannel, buffer );

            // Then
            assertThat( outcome, equalTo( PROTOCOL_CHOSEN ) );
            assertThat( handshake.chosenProtocol(), equalTo( protocol ) );
            assertThat( buffer.readableBytes(), equalTo( 4 ) );
        }
    }

    @Test
    public void shouldHandleVersionBoundary()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, ctx, messageLogger ) )
        {
            // Given
            long maxUnsignedInt32 = 4_294_967_295L;
            BoltProtocolHandlerFactory handlerFactory = newHandlerFactory( maxUnsignedInt32, protocol );
            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( handlerFactory, false, true );

            // When
            HandshakeOutcome outcome = handshake.perform( boltChannel, wrappedBuffer( new byte[]{
                    (byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17,
                    (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                    0, 0, 0, 0,
                    0, 0, 0, 0,
                    0, 0, 0, 0 } ) );

            // Then
            assertThat( outcome, equalTo( PROTOCOL_CHOSEN ) );
            assertThat( handshake.chosenProtocol(), equalTo( protocol ) );
        }
    }

    @Test
    public void shouldFallBackToNoneProtocolIfNoMatch()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, ctx, messageLogger ) )
        {
            // Given
            BoltProtocolHandlerFactory handlerFactory = newHandlerFactory( 1, protocol );
            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( handlerFactory, false, true );

            // When
            HandshakeOutcome outcome = handshake.perform( boltChannel, wrappedBuffer( new byte[]{
                    (byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17,
                    0, 0, 0, 0,
                    0, 0, 0, 2,
                    0, 0, 0, 3,
                    0, 0, 0, 4 } ) );

            // Then
            assertThat( outcome, equalTo( NO_APPLICABLE_PROTOCOL ) );
            assertThat( handshake.chosenProtocol(), nullValue() );
        }
    }

    @Test
    public void shouldRejectIfInvalidHandshake()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, ctx, messageLogger ) )
        {
            // Given
            BoltProtocolHandlerFactory handlerFactory = newHandlerFactory( 1, protocol );
            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( handlerFactory, false, true );

            // When
            HandshakeOutcome outcome = handshake.perform( boltChannel, wrappedBuffer( new byte[]{
                    (byte) 0xDE, (byte) 0xAD, (byte) 0xB0, (byte) 0x17,
                    0, 0, 0, 1,
                    0, 0, 0, 0,
                    0, 0, 0, 0,
                    0, 0, 0, 0 } ) );

            // Then
            assertThat( outcome, equalTo( INVALID_HANDSHAKE ) );
            assertThat( handshake.chosenProtocol(), nullValue() );
        }
    }

    @Test
    public void shouldRejectIfInsecureHandshake()
    {
        try ( BoltChannel boltChannel = BoltChannel.open( connector, ctx, messageLogger ) )
        {
            // Given
            BoltProtocolHandlerFactory handlerFactory = newHandlerFactory( 1, protocol );
            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( handlerFactory, true, false );

            // When
            HandshakeOutcome outcome = handshake.perform( boltChannel, wrappedBuffer( new byte[]{
                    (byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17,
                    0, 0, 0, 1,
                    0, 0, 0, 0,
                    0, 0, 0, 0,
                    0, 0, 0, 0 } ) );

            // Then
            assertThat( outcome, equalTo( INSECURE_HANDSHAKE ) );
            assertThat( handshake.chosenProtocol(), nullValue() );
        }
    }

    private static BoltProtocolHandlerFactory newHandlerFactory( long version, BoltMessagingProtocolHandler handler )
    {
        return ( givenVersion, channel ) -> version == givenVersion ? handler : null;
    }

    private static ChannelHandlerContext newChannelHandlerContextMock()
    {
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );
        Channel channel = mock( Channel.class );
        when( context.channel() ).thenReturn( channel );
        return context;
    }
}
