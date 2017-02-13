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
import io.netty.channel.Channel;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import org.neo4j.bolt.transport.BoltProtocol;
import org.neo4j.bolt.transport.HandshakeOutcome;
import org.neo4j.bolt.transport.ProtocolChooser;

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

public class ProtocolChooserTest
{
    private final Map<Long, BiFunction<Channel, Boolean, BoltProtocol>> available = new HashMap<>();
    private final BiFunction factory = mock( BiFunction.class );
    private final BoltProtocol protocol = mock( BoltProtocol.class );
    private final Channel ch = mock(Channel.class);

    @Test
    public void shouldChooseFirstAvailableProtocol() throws Throwable
    {
        // Given
        when( factory.apply( ch, true ) ).thenReturn( protocol );
        available.put( 1L, factory );

        ProtocolChooser chooser = new ProtocolChooser( available, false, true );

        // When
        HandshakeOutcome outcome =
                chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
                        (byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17,
                        0, 0, 0, 0,
                        0, 0, 0, 1,
                        0, 0, 0, 0,
                        0, 0, 0, 0} ), ch );

        // Then
        assertThat( outcome, equalTo( PROTOCOL_CHOSEN ) );
        assertThat( chooser.chosenProtocol(), equalTo( protocol ) );
    }

    @Test
    public void shouldHandleFragmentedMessage() throws Throwable
    {
        // Given
        when( factory.apply( ch, true ) ).thenReturn( protocol );
        available.put( 1L, factory );

        ProtocolChooser chooser = new ProtocolChooser( available, false, true );

        // When
        HandshakeOutcome firstOutcome = chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
                (byte) 0x60, (byte) 0x60} ), ch );
        // When
        HandshakeOutcome secondOutcome = chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
                (byte) 0xB0, (byte) 0x17 } ), ch );
        HandshakeOutcome thirdOutcome = chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
                0, 0, 0, 0,
                0, 0, 0} ), ch );
        HandshakeOutcome fourthOutcome = chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
                1,
                0, 0, 0, 0,
                0, 0, 0, 0} ), ch );

        // Then
        assertThat( firstOutcome, equalTo( PARTIAL_HANDSHAKE ) );
        assertThat( secondOutcome, equalTo( PARTIAL_HANDSHAKE ) );
        assertThat( thirdOutcome, equalTo( PARTIAL_HANDSHAKE ) );
        assertThat( fourthOutcome, equalTo( PROTOCOL_CHOSEN ) );
        assertThat( chooser.chosenProtocol(), equalTo( protocol ) );
    }

    @Test
    public void shouldHandleHandshakeFollowedByMessageInSameBuffer() throws Throwable
    {
        // Given
        when( factory.apply( ch, true ) ).thenReturn( protocol );
        available.put( 1L, factory );

        ProtocolChooser chooser = new ProtocolChooser( available, false, true );

        // When
        ByteBuf buffer = wrappedBuffer( new byte[]{
                (byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17,
                0, 0, 0, 0,
                0, 0, 0, 1,
                0, 0, 0, 0,
                0, 0, 0, 0,
                1, 2, 3, 4} ); // These last four bytes are not part of the handshake

        HandshakeOutcome outcome = chooser.handleVersionHandshakeChunk( buffer, ch );

        // Then
        assertThat( outcome, equalTo( PROTOCOL_CHOSEN ) );
        assertThat( chooser.chosenProtocol(), equalTo( protocol ) );
        assertThat( buffer.readableBytes(), equalTo( 4 ) );
    }

    @Test
    public void shouldHandleVersionBoundary() throws Throwable
    {
        // Given
        long maxUnsignedInt32 = 4_294_967_295L;

        when( factory.apply( ch, true ) ).thenReturn( protocol );
        available.put( maxUnsignedInt32, factory );

        ProtocolChooser chooser = new ProtocolChooser( available, false, true );

        // When
        HandshakeOutcome outcome = chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
                (byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17,
                (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0} ), ch );

        // Then
        assertThat( outcome, equalTo( PROTOCOL_CHOSEN ) );
        assertThat( chooser.chosenProtocol(), equalTo( protocol ) );
    }

    @Test
    public void shouldFallBackToNoneProtocolIfNoMatch() throws Throwable
    {
        // Given
        when( factory.apply( ch, true ) ).thenReturn( protocol );

        available.put( 1L, mock( BiFunction.class ) );

        ProtocolChooser chooser = new ProtocolChooser( available, false, true );

        // When
        HandshakeOutcome outcome = chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
                (byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17,
                0, 0, 0, 0,
                0, 0, 0, 2,
                0, 0, 0, 3,
                0, 0, 0, 4} ), ch );

        // Then
        assertThat( outcome, equalTo( NO_APPLICABLE_PROTOCOL ) );
        assertThat( chooser.chosenProtocol(), nullValue() );
    }

    @Test
    public void shouldRejectIfInvalidHandshake() throws Throwable
    {
        // Given
        when( factory.apply( ch, true ) ).thenReturn( protocol );

        available.put( 1L, mock( BiFunction.class ) );

        ProtocolChooser chooser = new ProtocolChooser( available, false, true );

        // When
        HandshakeOutcome outcome = chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
                (byte) 0xDE, (byte) 0xAD, (byte) 0xB0, (byte) 0x17,
                0, 0, 0, 1,
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0} ), ch );

        // Then
        assertThat( outcome, equalTo( INVALID_HANDSHAKE ) );
        assertThat( chooser.chosenProtocol(), nullValue() );
    }

    @Test
    public void shouldRejectIfInsecureHandshake() throws Throwable
    {
        // Given
        when( factory.apply( ch, true ) ).thenReturn( protocol );

        available.put( 1L, mock( BiFunction.class ) );

        ProtocolChooser chooser = new ProtocolChooser( available, true, false );

        // When
        HandshakeOutcome outcome = chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
                (byte) 0x60, (byte) 0x60, (byte) 0xB0, (byte) 0x17,
                0, 0, 0, 1,
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0} ), ch );

        // Then
        assertThat( outcome, equalTo( INSECURE_HANDSHAKE ) );
        assertThat( chooser.chosenProtocol(), nullValue() );
    }
}
