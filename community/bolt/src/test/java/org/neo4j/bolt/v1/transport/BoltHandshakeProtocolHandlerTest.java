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

package org.neo4j.bolt.v1.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltMessageLog;
import org.neo4j.bolt.BoltMessageLogger;
import org.neo4j.bolt.transport.BoltMessagingProtocolHandler;
import org.neo4j.bolt.transport.HandshakeOutcome;
import org.neo4j.bolt.transport.BoltHandshakeProtocolHandler;

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
    private final Map<Long, Function<BoltChannel, BoltMessagingProtocolHandler>> protocolHandlers = new HashMap<>();
    private final Function factory = mock( Function.class );
    private final BoltMessagingProtocolHandler protocol = mock( BoltMessagingProtocolHandler.class );
    private final ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
    private final BoltMessageLog messageLog = BoltMessageLog.getInstance();
    private final BoltMessageLogger messageLogger = new BoltMessageLogger( messageLog, ctx.channel() );

    @Test
    public void shouldChooseFirstAvailableProtocol() throws Throwable
    {
        try ( BoltChannel boltChannel = BoltChannel.open( ctx, messageLogger ) )
        {
            // Given
            when( factory.apply( boltChannel ) ).thenReturn( protocol );
            protocolHandlers.put( 1L, factory );

            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( protocolHandlers, false, true );

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
    public void shouldHandleFragmentedMessage() throws Throwable
    {
        try ( BoltChannel boltChannel = BoltChannel.open( ctx, messageLogger ) )
        {
            // Given
            when( factory.apply( boltChannel ) ).thenReturn( protocol );
            protocolHandlers.put( 1L, factory );

            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( protocolHandlers, false, true );

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
    public void shouldHandleHandshakeFollowedByMessageInSameBuffer() throws Throwable
    {
        try ( BoltChannel boltChannel = BoltChannel.open( ctx, messageLogger ) )
        {
            // Given
            when( factory.apply( boltChannel ) ).thenReturn( protocol );
            protocolHandlers.put( 1L, factory );

            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( protocolHandlers, false, true );

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
    public void shouldHandleVersionBoundary() throws Throwable
    {
        try ( BoltChannel boltChannel = BoltChannel.open( ctx, messageLogger ) )
        {
            // Given
            long maxUnsignedInt32 = 4_294_967_295L;

            when( factory.apply( boltChannel ) ).thenReturn( protocol );
            protocolHandlers.put( maxUnsignedInt32, factory );

            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( protocolHandlers, false, true );

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
    public void shouldFallBackToNoneProtocolIfNoMatch() throws Throwable
    {
        try ( BoltChannel boltChannel = BoltChannel.open( ctx, messageLogger ) )
        {
            // Given
            when( factory.apply( boltChannel ) ).thenReturn( protocol );

            protocolHandlers.put( 1L, mock( Function.class ) );

            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( protocolHandlers, false, true );

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
    public void shouldRejectIfInvalidHandshake() throws Throwable
    {
        try ( BoltChannel boltChannel = BoltChannel.open( ctx, messageLogger ) )
        {
            // Given
            when( factory.apply( boltChannel ) ).thenReturn( protocol );

            protocolHandlers.put( 1L, mock( Function.class ) );

            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( protocolHandlers, false, true );

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
    public void shouldRejectIfInsecureHandshake() throws Throwable
    {
        try ( BoltChannel boltChannel = BoltChannel.open( ctx, messageLogger ) )
        {
            // Given
            when( factory.apply( boltChannel ) ).thenReturn( protocol );

            protocolHandlers.put( 1L, mock( Function.class ) );

            BoltHandshakeProtocolHandler handshake = new BoltHandshakeProtocolHandler( protocolHandlers, true, false );

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
}
