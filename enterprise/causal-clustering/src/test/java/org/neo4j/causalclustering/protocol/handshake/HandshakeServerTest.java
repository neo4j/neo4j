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

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.causalclustering.protocol.Protocol.Identifier.RAFT;
import static org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolResponse.NO_PROTOCOL;
import static org.neo4j.causalclustering.protocol.handshake.StatusCode.FAILURE;
import static org.neo4j.causalclustering.protocol.handshake.StatusCode.SUCCESS;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.CATCHUP_1;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.RAFT_1;
import static org.neo4j.causalclustering.protocol.handshake.TestProtocols.RAFT_3;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class HandshakeServerTest
{
    private ProtocolHandshakeTest.FakeChannelWrapper channel = mock( ProtocolHandshakeTest.FakeChannelWrapper.class );
    private ProtocolRepository protocolRepository = new ProtocolRepository( TestProtocols.values() );

    private HandshakeServer server = new HandshakeServer( channel, protocolRepository, RAFT );

    @Test
    public void shouldDeclineUnallowedProtocol()
    {
        // given
        server.handle( new InitialMagicMessage() );

        // when
        server.handle( new ApplicationProtocolRequest( CATCHUP_1.identifier(), asSet( CATCHUP_1.version() ) ) );

        // then
        verify( channel ).dispose();
    }

    @Test
    public void shouldDisconnectOnWrongMagicValue()
    {
        // when
        server.handle( new InitialMagicMessage( "PLAIN_VALUE" ) );

        // then
        verify( channel ).dispose();
    }

    @Test
    public void shouldAcceptCorrectMagicValue()
    {
        // when
        server.handle( new InitialMagicMessage() );

        // then
        verify( channel, never() ).dispose();
    }

    @Test
    public void shouldSendProtocolResponseForGivenProtocol()
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( new InitialMagicMessage() );

        // when
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), versions ) );

        // then
        verify( channel ).writeAndFlush( new ApplicationProtocolResponse( SUCCESS, RAFT_3.identifier(), RAFT_3.version() ) );
    }

    @Test
    public void shouldNotCloseConnectionIfKnownProtocol()
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( new InitialMagicMessage() );

        // when
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), versions ) );

        // then
        verify( channel, never() ).dispose();
    }

    @Test
    public void shouldSendNegativeResponseAndCloseForUnknownProtocol()
    {
        // given
        Set<Integer> versions = asSet( 1, 2, 3 );
        server.handle( new InitialMagicMessage() );

        // when
        server.handle( new ApplicationProtocolRequest( "UNKNOWN", versions ) );

        // then
        InOrder inOrder = inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( NO_PROTOCOL );
        inOrder.verify( channel ).dispose();
    }

    @Test
    public void shouldNotSetProtocolStackForUnknownProtocol()
    {
        assertThrows( ServerHandshakeException.class, () -> {
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
        } );
    }

    @Test
    public void shouldSendFailureOnUnknownProtocolSwitchOver()
    {
        // given
        int version = 1;
        String unknownProtocolName = "UNKNOWN";
        server.handle( new InitialMagicMessage() );
        server.handle( new ApplicationProtocolRequest( unknownProtocolName, asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest( unknownProtocolName, version ) );

        // then
        InOrder inOrder = inOrder( channel );
        inOrder.verify( channel ).writeAndFlush( new SwitchOverResponse( FAILURE ) );
        inOrder.verify( channel ).dispose();
    }

    @Test
    public void shouldCompleteProtocolStackOnSuccessfulSwitchOver() throws Exception
    {
        // given
        int version = 1;
        server.handle( new InitialMagicMessage() );
        server.handle( new ApplicationProtocolRequest( RAFT.canonicalName(), asSet( version ) ) );

        // when
        server.handle( new SwitchOverRequest( RAFT_1.identifier(), version ) );

        // then
        verify( channel ).writeAndFlush( new InitialMagicMessage() );
        verify( channel ).writeAndFlush( new SwitchOverResponse( SUCCESS ) );
        assertThat( server.protocolStackFuture().get( 1, SECONDS ), equalTo( new ProtocolStack( RAFT_1 ) ) );
    }
}
