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

// TODO: modifier protocols
public class HandshakeClient implements ClientMessageHandler
{
    private Channel channel;
    private ProtocolRepository protocolRepository;
    private ProtocolSelection knownProtocolVersions;
    private ProtocolStack protocolStack;
    private CompletableFuture<ProtocolStack> future = new CompletableFuture<>();
    private boolean magicReceived;

    public CompletableFuture<ProtocolStack> initiate( Channel channel, ProtocolRepository protocolRepository, Protocol.Identifier protocol )
    {
        this.channel = channel;
        this.protocolRepository = protocolRepository;
        this.knownProtocolVersions = protocolRepository.getAll( protocol );

        channel.write( new InitialMagicMessage() );
        channel.writeAndFlush( new ApplicationProtocolRequest( knownProtocolVersions.identifier(), knownProtocolVersions.versions() ) );

        return future;
    }

    private void ensureMagic()
    {
        if ( !magicReceived )
        {
            decline( "Magic value not received." );
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
    public void handle( ApplicationProtocolResponse applicationProtocolResponse )
    {
        ensureMagic();

        if ( applicationProtocolResponse.statusCode() != SUCCESS )
        {
            decline( "Unsuccessful application protocol response" );
            return;
        }
        if ( !knownProtocolVersions.identifier().equals( applicationProtocolResponse.protocolName() ) )
        {
            decline( String.format( "Mismatch of protocol name from client %s and server %s",
                    knownProtocolVersions.identifier(), applicationProtocolResponse.protocolName() ) );
            return;
        }
        if ( knownProtocolVersions.versions().stream().noneMatch( version -> version == applicationProtocolResponse.version() ) )
        {
            decline( String.format( "Mismatch of protocol versions for protocol %s from client %s and server %s", knownProtocolVersions.identifier(),
                    knownProtocolVersions.versions(), applicationProtocolResponse.version() ) );
            return;
        }

        Optional<Protocol> protocol = protocolRepository.select( applicationProtocolResponse.protocolName(), applicationProtocolResponse.version() );

        if ( !protocol.isPresent() )
        {
            throw new IllegalStateException( "Asked protocol must exist in local repository" );
        }

        Protocol applicationProtocol = protocol.get();
        protocolStack = new ProtocolStack( applicationProtocol );

        channel.writeAndFlush( new SwitchOverRequest( applicationProtocol.identifier(), applicationProtocol.version() ) );
    }

    @Override
    public void handle( ModifierProtocolResponse modifierProtocolResponse )
    {
        ensureMagic();
        throw new UnsupportedOperationException( "Not implemented" );
    }

    @Override
    public void handle( SwitchOverResponse response )
    {
        ensureMagic();

        if ( protocolStack == null )
        {
            decline( "Attempted to switch over when protocol stack not established" );
            return;
        }

        // TODO: modifier protocols

        future.complete( protocolStack );
    }

    boolean failIfNotDone( String message )
    {
        if ( !future.isDone() )
        {
            decline( message );
            return true;
        }
        return false;
    }

    private void decline( String message )
    {
        future.completeExceptionally( new ClientHandshakeException( message ) );
    }
}
