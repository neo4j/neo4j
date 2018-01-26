/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.protocol.handshake;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.protocol.Protocol;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.causalclustering.protocol.handshake.StatusCode.FAILURE;
import static org.neo4j.causalclustering.protocol.handshake.StatusCode.SUCCESS;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class HandshakeServerTest
{
    private ProtocolHandshakeTest.FakeChannelWrapper channel = mock( ProtocolHandshakeTest.FakeChannelWrapper.class );
    private ProtocolRepository protocolRepository = new ProtocolRepository( TestProtocols.values() );

    private HandshakeServer server = new HandshakeServer( channel, protocolRepository, Protocol.Identifier.RAFT );

    @Test
    public void shouldDeclineUnallowedProtocol() throws Exception
    {
        // given
        server.handle( new InitialMagicMessage() );

        // when
        server.handle( new ApplicationProtocolRequest( TestProtocols.CATCHUP_1.identifier(), asSet( TestProtocols.CATCHUP_1.version() ) ) );

        // then
        verify( channel ).dispose();
    }

    @Test
    public void shouldDisconnectOnWrongMagicValue() throws Exception
    {
        // when
        server.handle( new InitialMagicMessage( "PLAIN_VALUE" ) );

        // then
        verify( channel ).dispose();
    }

    @Test
    public void shouldAcceptCorrectMagicValue() throws Exception
    {
        // when
        server.handle( new InitialMagicMessage() );

        // then
        verify( channel, never() ).dispose();
    }

    @Test
    public void shouldSendProtocolResponseForGivenProtocol() throws Throwable
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( new InitialMagicMessage() );

        // when
        server.handle( new ApplicationProtocolRequest( TestProtocols.Identifier.RAFT.canonicalName(), versions ) );

        // then
        verify( channel ).writeAndFlush( new ApplicationProtocolResponse( SUCCESS, TestProtocols.RAFT_3.identifier(), TestProtocols.RAFT_3.version() ) );
    }

    @Test
    public void shouldNotCloseConnectionIfKnownProtocol() throws Throwable
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( new InitialMagicMessage() );

        // when
        server.handle( new ApplicationProtocolRequest( TestProtocols.Identifier.RAFT.canonicalName(), versions ) );

        // then
        verify( channel, never() ).dispose();
    }

    @Test
    public void shouldSendNegativeResponseAndCloseForUnknownProtocol() throws Throwable
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( new InitialMagicMessage() );

        // when
        server.handle( new ApplicationProtocolRequest( "UNKNOWN", versions ) );

        // then
        InOrder inOrder = Mockito.inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( ApplicationProtocolResponse.NO_PROTOCOL );
        inOrder.verify( channel ).dispose();
    }

    @Test( expected = ServerHandshakeException.class )
    public void shouldNotSetProtocolStackForUnknownProtocol() throws Throwable
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( new InitialMagicMessage() );

        // when
        server.handle( new ApplicationProtocolRequest( "UNKNOWN", versions ) );

        // then
        try
        {
            server.protocolStackFuture().get();
            fail( "Expected failure" );
        }
        catch ( ExecutionException ex )
        {
            throw ex.getCause();
        }
    }

    @Test
    public void shouldSendFailureOnUnknownProtocolSwitchOver() throws Exception
    {
        // given
        int version = 1;
        String unknownProtocolName = "UNKNOWN";
        server.handle( new InitialMagicMessage() );
        server.handle( new ApplicationProtocolRequest( unknownProtocolName, asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest( unknownProtocolName, version ) );

        // then
        InOrder inOrder = Mockito.inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    public void shouldCompleteProtocolStackOnSuccessfulSwitchOver() throws Exception
    {
        // given
        int version = 1;
        server.handle( new InitialMagicMessage() );
        server.handle( new ApplicationProtocolRequest( TestProtocols.Identifier.RAFT.canonicalName(), asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest( TestProtocols.RAFT_1.identifier(), version ) );

        // then
        verify( channel ).writeAndFlush( new InitialMagicMessage() );
        verify( channel ).writeAndFlush( new SwitchOverResponse( SUCCESS ) );
        assertThat( server.protocolStackFuture().get( 1, TimeUnit.SECONDS ), equalTo( new ProtocolStack( TestProtocols.RAFT_1 ) ) );
    }
}
