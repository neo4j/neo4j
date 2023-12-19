/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.protocol.handshake;

import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.neo4j.causalclustering.messaging.Channel;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;

import static java.util.Collections.emptyList;

/**
 * @see ProtocolHandshakeHappyTest happy path tests
 */
public class ProtocolHandshakeSadTest
{
    private ApplicationSupportedProtocols supportedRaftApplicationProtocol =
            new ApplicationSupportedProtocols( Protocol.ApplicationProtocolCategory.RAFT, emptyList() );
    private ApplicationSupportedProtocols supportedCatchupApplicationProtocol =
            new ApplicationSupportedProtocols( Protocol.ApplicationProtocolCategory.CATCHUP, emptyList() );
    private Collection<ModifierSupportedProtocols> noModifiers = emptyList();

    private ApplicationProtocolRepository raftApplicationProtocolRepository =
            new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportedRaftApplicationProtocol );
    private ApplicationProtocolRepository catchupApplicationProtocolRepository =
            new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportedCatchupApplicationProtocol );
    private ModifierProtocolRepository modifierProtocolRepository =
            new ModifierProtocolRepository( TestModifierProtocols.values(), noModifiers );

    private HandshakeClient handshakeClient = new HandshakeClient();

    @Test( expected = ClientHandshakeException.class )
    public void shouldFailHandshakeForUnknownProtocolOnClient() throws Throwable
    {
        // given
        HandshakeServer handshakeServer = new HandshakeServer(
                raftApplicationProtocolRepository, modifierProtocolRepository, new ProtocolHandshakeHappyTest.FakeServerChannel( handshakeClient )
        );
        Channel clientChannel = new ProtocolHandshakeHappyTest.FakeClientChannel( handshakeServer );

        // when
        CompletableFuture<ProtocolStack> clientHandshakeFuture = handshakeClient.initiate(
                clientChannel, catchupApplicationProtocolRepository, modifierProtocolRepository );

        // then
        try
        {
            clientHandshakeFuture.getNow( null );
        }
        catch ( CompletionException ex )
        {
            throw ex.getCause();
        }
    }

    @Test( expected = ServerHandshakeException.class )
    public void shouldFailHandshakeForUnknownProtocolOnServer() throws Throwable
    {
        // given
        HandshakeServer handshakeServer = new HandshakeServer(
                raftApplicationProtocolRepository, modifierProtocolRepository, new ProtocolHandshakeHappyTest.FakeServerChannel( handshakeClient ) );
        Channel clientChannel = new ProtocolHandshakeHappyTest.FakeClientChannel( handshakeServer );

        // when
        handshakeClient.initiate( clientChannel, catchupApplicationProtocolRepository, modifierProtocolRepository );
        CompletableFuture<ProtocolStack> serverHandshakeFuture = handshakeServer.protocolStackFuture();

        // then
        try
        {
            serverHandshakeFuture.getNow( null );
        }
        catch ( CompletionException ex )
        {
            throw ex.getCause();
        }
    }
}
