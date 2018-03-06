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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.messaging.Channel;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.stream.Streams;

import static org.neo4j.causalclustering.protocol.handshake.StatusCode.SUCCESS;

public class HandshakeClient implements ClientMessageHandler
{
    private Channel channel;
    private ProtocolRepository<Protocol.ApplicationProtocol> applicationProtocolRepository;
    private ProtocolRepository<Protocol.ModifierProtocol> modifierProtocolRepository;
    private ProtocolSelection<Protocol.ApplicationProtocol> knownApplicationProtocolVersions;
    private List<ProtocolSelection<Protocol.ModifierProtocol>> knownModifierProtocolVersions;
    private Protocol.ApplicationProtocol applicationProtocol;
    private Map<String,Optional<Protocol.ModifierProtocol>> negotiatedModifierProtocols = new HashMap<>();
    private ProtocolStack protocolStack;
    private CompletableFuture<ProtocolStack> future = new CompletableFuture<>();
    private boolean magicReceived;

    public CompletableFuture<ProtocolStack> initiate( Channel channel, ProtocolRepository<Protocol.ApplicationProtocol> applicationProtocolRepository,
            Protocol.ApplicationProtocolIdentifier applicationProtocolIdentifier, ProtocolRepository<Protocol.ModifierProtocol> modifierProtocolRepository,
            Set<Protocol.ModifierProtocolIdentifier> modifierProtocolIdentifiers )
    {
        this.channel = channel;
        this.applicationProtocolRepository = applicationProtocolRepository;
        this.knownApplicationProtocolVersions = applicationProtocolRepository.getAll( applicationProtocolIdentifier );

        this.modifierProtocolRepository = modifierProtocolRepository;
        this.knownModifierProtocolVersions = modifierProtocolIdentifiers
                .stream()
                .map( modifierProtocolRepository::getAll )
                .collect( Collectors.toList() );

        channel.write( InitialMagicMessage.instance() );

        knownModifierProtocolVersions.forEach( modifierProtocolSelection ->
                channel.write( new ModifierProtocolRequest( modifierProtocolSelection.identifier(), modifierProtocolSelection .versions() ) ) );

        channel.writeAndFlush( new ApplicationProtocolRequest( knownApplicationProtocolVersions.identifier(), knownApplicationProtocolVersions.versions() ) );

        return future;
    }

    private void ensureMagic()
    {
        if ( !magicReceived )
        {
            decline( "Magic value not received." );
            throw new IllegalStateException( "Magic value not received." );
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

        Optional<Protocol.ApplicationProtocol> protocol =
                applicationProtocolRepository.select( applicationProtocolResponse.protocolName(), applicationProtocolResponse.version() );

        if ( !protocol.isPresent() )
        {
            decline( String.format(
                    "Mismatch of application protocols between client and server: Server protocol %s version %d: Client protocol %s versions %s",
                    applicationProtocolResponse.protocolName(), applicationProtocolResponse.version(),
                    knownApplicationProtocolVersions.identifier(), knownApplicationProtocolVersions.versions() ) );
        }
        else
        {
            applicationProtocol = protocol.get();

            sendSwitchOverRequestIfReady();
        }
    }

    @Override
    public void handle( ModifierProtocolResponse modifierProtocolResponse )
    {
        ensureMagic();
        if ( modifierProtocolResponse.statusCode() == StatusCode.SUCCESS )
        {
            Optional<Protocol.ModifierProtocol> selectedModifierProtocol =
                    modifierProtocolRepository.select( modifierProtocolResponse.protocolName(), modifierProtocolResponse.version() );
            negotiatedModifierProtocols.put( modifierProtocolResponse.protocolName(), selectedModifierProtocol );
        }
        else
        {
            negotiatedModifierProtocols.put( modifierProtocolResponse.protocolName(), Optional.empty() );
        }

        sendSwitchOverRequestIfReady();
    }

    private void sendSwitchOverRequestIfReady()
    {
        if ( applicationProtocol != null && negotiatedModifierProtocols.size() == knownModifierProtocolVersions.size() )
        {
            List<Protocol.ModifierProtocol> agreedModifierProtocols = negotiatedModifierProtocols
                    .values()
                    .stream()
                    .flatMap( Streams::ofOptional )
                    .collect( Collectors.toList() );

            protocolStack = new ProtocolStack( applicationProtocol, agreedModifierProtocols );
            List<Pair<String,Integer>> switchOverModifierProtocols =
                    agreedModifierProtocols
                            .stream()
                            .map( protocol -> Pair.of( protocol.identifier(), protocol.version() ) )
                            .collect( Collectors.toList() );

            channel.writeAndFlush( new SwitchOverRequest( applicationProtocol.identifier(), applicationProtocol.version(), switchOverModifierProtocols ) );
        }
    }

    @Override
    public void handle( SwitchOverResponse response )
    {
        ensureMagic();
        if ( protocolStack == null )
        {
            decline( "Attempted to switch over when protocol stack not established" );
        }
        else if ( response.status() != StatusCode.SUCCESS )
        {
            decline( "Server failed to switch over" );
        }
        else
        {
            future.complete( protocolStack );
        }
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
