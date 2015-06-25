/*
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
package org.neo4j.ndp.transport.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.function.Function;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.ndp.transport.socket.SocketTransportHandler.HandshakeOutcome;
import static org.neo4j.ndp.transport.socket.SocketTransportHandler.HandshakeOutcome.NO_APPLICABLE_PROTOCOL;
import static org.neo4j.ndp.transport.socket.SocketTransportHandler.HandshakeOutcome.PARTIAL_HANDSHAKE;
import static org.neo4j.ndp.transport.socket.SocketTransportHandler.HandshakeOutcome.PROTOCOL_CHOSEN;
import static org.neo4j.ndp.transport.socket.SocketTransportHandler.ProtocolChooser;

public class ProtocolChooserTest
{
    private final PrimitiveLongObjectMap<Function<Channel, SocketProtocol>> available = Primitive.longObjectMap();
    private final Function factory = mock( Function.class );
    private final SocketProtocol protocol = mock( SocketProtocol.class );
    private final Channel ch = mock(Channel.class);

    @Test
    public void shouldChooseFirstAvailableProtocol() throws Throwable
    {
        // Given
        when( factory.apply(ch) ).thenReturn( protocol );
        available.put( 1, factory );

        ProtocolChooser chooser = new ProtocolChooser( available );

        // When
        HandshakeOutcome outcome =
                chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
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
        when( factory.apply( ch ) ).thenReturn( protocol );
        available.put( 1, factory );

        ProtocolChooser chooser = new ProtocolChooser( available );

        // When
        HandshakeOutcome firstOutcome = chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
                0, 0, 0, 0,
                0, 0, 0} ), ch );
        HandshakeOutcome secondOutcome = chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
                1,
                0, 0, 0, 0,
                0, 0, 0, 0} ), ch );

        // Then
        assertThat( firstOutcome, equalTo( PARTIAL_HANDSHAKE ) );
        assertThat( secondOutcome, equalTo( PROTOCOL_CHOSEN ) );
        assertThat( chooser.chosenProtocol(), equalTo( protocol ) );
    }

    @Test
    public void shouldHandleHandshakeFollowedByMessageInSameBuffer() throws Throwable
    {
        // Given
        when( factory.apply(ch) ).thenReturn( protocol );
        available.put( 1, factory );

        ProtocolChooser chooser = new ProtocolChooser( available );

        // When
        ByteBuf buffer = wrappedBuffer( new byte[]{
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
        long maxUnsignedInt32 = 4_294_967_295l;

        when( factory.apply( ch ) ).thenReturn( protocol );
        available.put( maxUnsignedInt32, factory );

        ProtocolChooser chooser = new ProtocolChooser( available );

        // When
        HandshakeOutcome outcome = chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
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
        when( factory.apply( ch ) ).thenReturn( protocol );

        available.put( 1, mock( Function.class ) );

        ProtocolChooser chooser = new ProtocolChooser( available );

        // When
        HandshakeOutcome outcome = chooser.handleVersionHandshakeChunk( wrappedBuffer( new byte[]{
                0, 0, 0, 0,
                0, 0, 0, 2,
                0, 0, 0, 3,
                0, 0, 0, 4} ), ch );

        // Then
        assertThat( outcome, equalTo( NO_APPLICABLE_PROTOCOL ) );
        assertThat( chooser.chosenProtocol(), nullValue() );
    }
}