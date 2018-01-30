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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.messaging.Channel;
import org.neo4j.causalclustering.protocol.Protocol;

import static org.neo4j.causalclustering.protocol.handshake.StatusCode.SUCCESS;

public class HandshakeServer implements ServerMessageHandler
{
    private final Channel channel;
    private final ProtocolRepository protocolRepository;
    private final Protocol.Identifier allowedProtocol;
    private ProtocolStack protocolStack;
    private final CompletableFuture<ProtocolStack> protocolStackFuture = new CompletableFuture<>();
    private boolean magicReceived;
    private boolean initialised;

    HandshakeServer( Channel channel, ProtocolRepository protocolRepository, Protocol.Identifier allowedProtocol )
    {
        this.channel = channel;
        this.protocolRepository = protocolRepository;
        this.allowedProtocol = allowedProtocol;
    }

    public void init()
    {
        channel.writeAndFlush( new InitialMagicMessage() );
        initialised = true;
    }

    private void ensureMagic()
    {
        if ( !magicReceived )
        {
            decline( "No magic value received" );
            throw new IllegalStateException( "Magic value not received." );
        }
        if ( !initialised )
        {
            init();
        }
    }

    @Override
    public void handle( InitialMagicMessage magicMessage )
    {
        if ( !magicMessage.isCorrectMagic() )
        {
            decline( "Incorrect magic value received" );
        }
        // TODO: check clusterId as well

        magicReceived = true;
    }

    @Override
    public void handle( ApplicationProtocolRequest request )
    {
        ensureMagic();

        ApplicationProtocolResponse response;
        if ( !request.protocolName().equals( allowedProtocol.canonicalName() ) )
        {
            response = ApplicationProtocolResponse.NO_PROTOCOL;
            channel.writeAndFlush( response );
            decline( String.format( "Requested protocol %s not supported", request.protocolName() ) );
            return;
        }

        Optional<Protocol> selected = protocolRepository.select( request.protocolName(), request.versions() );

        if ( selected.isPresent() )
        {
            Protocol selectedProtocol = selected.get();
            protocolStack = new ProtocolStack( selectedProtocol );
            response = new ApplicationProtocolResponse( SUCCESS, selectedProtocol.identifier(), selectedProtocol.version() );
            channel.writeAndFlush( response );
        }
        else
        {
            response = ApplicationProtocolResponse.NO_PROTOCOL;
            channel.writeAndFlush( response );
            decline( String.format( "Do not support requested protocol %s versions %s", request.protocolName(), request.versions() ) );
        }
    }

    @Override
    public void handle( ModifierProtocolRequest modifierProtocolRequest )
    {
        ensureMagic();
        throw new UnsupportedOperationException( "Not implemented" );
    }

    @Override
    public void handle( SwitchOverRequest switchOverRequest )
    {
        ensureMagic();
        Optional<Protocol> switchOverProtocol = protocolRepository.select( switchOverRequest.protocolName(), switchOverRequest.version() );

        if ( !switchOverProtocol.isPresent() )
        {
            channel.writeAndFlush( SwitchOverResponse.FAILURE );
            decline( String.format( "Cannot switch to protocol %s version %d", switchOverRequest.protocolName(), switchOverRequest.version() ) );
            return;
        }
        else if ( !switchOverProtocol.get().equals( protocolStack.applicationProtocol() ) )
        {
            channel.writeAndFlush( SwitchOverResponse.FAILURE );
            decline( String.format( "Switch over mismatch: requested %s version %s but negotiated %s version %s",
                    switchOverRequest.protocolName(), switchOverRequest.version(),
                    protocolStack.applicationProtocol().identifier(), protocolStack.applicationProtocol().version() ) );
            return;
        }

        SwitchOverResponse response = new SwitchOverResponse( SUCCESS );
        channel.writeAndFlush( response );

        protocolStackFuture.complete( protocolStack );
    }

    private void decline( String message )
    {
        channel.dispose();
        protocolStackFuture.completeExceptionally( new ServerHandshakeException( message ) );
    }

    CompletableFuture<ProtocolStack> protocolStackFuture()
    {
        return protocolStackFuture;
    }
}
