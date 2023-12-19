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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.messaging.Channel;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocol;
import org.neo4j.stream.Streams;

import static org.neo4j.causalclustering.protocol.handshake.StatusCode.SUCCESS;

public class HandshakeServer implements ServerMessageHandler
{
    private final Channel channel;
    private final ApplicationProtocolRepository applicationProtocolRepository;
    private final ModifierProtocolRepository modifierProtocolRepository;
    private final SupportedProtocols<Integer,ApplicationProtocol> supportedApplicationProtocol;
    private final ProtocolStack.Builder protocolStackBuilder = ProtocolStack.builder();
    private final CompletableFuture<ProtocolStack> protocolStackFuture = new CompletableFuture<>();
    private boolean magicReceived;
    private boolean initialised;

    HandshakeServer( ApplicationProtocolRepository applicationProtocolRepository, ModifierProtocolRepository modifierProtocolRepository, Channel channel )
    {
        this.channel = channel;
        this.applicationProtocolRepository = applicationProtocolRepository;
        this.modifierProtocolRepository = modifierProtocolRepository;
        this.supportedApplicationProtocol = applicationProtocolRepository.supportedProtocol();
    }

    public void init()
    {
        channel.writeAndFlush( InitialMagicMessage.instance() );
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
        if ( !request.protocolName().equals( supportedApplicationProtocol.identifier().canonicalName() ) )
        {
            response = ApplicationProtocolResponse.NO_PROTOCOL;
            channel.writeAndFlush( response );
            decline( String.format( "Requested protocol %s not supported", request.protocolName() ) );
        }
        else
        {
            Optional<ApplicationProtocol> selected = applicationProtocolRepository.select( request.protocolName(), supportedVersionsFor( request ) );

            if ( selected.isPresent() )
            {
                ApplicationProtocol selectedProtocol = selected.get();
                protocolStackBuilder.application( selectedProtocol );
                response = new ApplicationProtocolResponse( SUCCESS, selectedProtocol.category(), selectedProtocol.implementation() );
                channel.writeAndFlush( response );
            }
            else
            {
                response = ApplicationProtocolResponse.NO_PROTOCOL;
                channel.writeAndFlush( response );
                decline( String.format( "Do not support requested protocol %s versions %s", request.protocolName(), request.versions() ) );
            }
        }
    }

    @Override
    public void handle( ModifierProtocolRequest modifierProtocolRequest )
    {
        ensureMagic();

        ModifierProtocolResponse response;
        Optional<ModifierProtocol> selected =
                modifierProtocolRepository.select( modifierProtocolRequest.protocolName(), supportedVersionsFor( modifierProtocolRequest ) );

        if ( selected.isPresent() )
        {
            ModifierProtocol modifierProtocol = selected.get();
            protocolStackBuilder.modifier( modifierProtocol );
            response = new ModifierProtocolResponse( SUCCESS, modifierProtocol.category(), modifierProtocol.implementation() );
        }
        else
        {
            response = ModifierProtocolResponse.failure( modifierProtocolRequest.protocolName() );
        }

        channel.writeAndFlush( response );
    }

    @Override
    public void handle( SwitchOverRequest switchOverRequest )
    {
        ensureMagic();
        ProtocolStack protocolStack = protocolStackBuilder.build();
        Optional<ApplicationProtocol> switchOverProtocol =
                applicationProtocolRepository.select( switchOverRequest.protocolName(), switchOverRequest.version() );
        List<ModifierProtocol> switchOverModifiers = switchOverRequest.modifierProtocols()
                .stream()
                .map( pair -> modifierProtocolRepository.select( pair.first(), pair.other() ) )
                .flatMap( Streams::ofOptional )
                .collect( Collectors.toList() );

        if ( !switchOverProtocol.isPresent() )
        {
            channel.writeAndFlush( SwitchOverResponse.FAILURE );
            decline( String.format( "Cannot switch to protocol %s version %d", switchOverRequest.protocolName(), switchOverRequest.version() ) );
        }
        else if ( protocolStack.applicationProtocol() == null )
        {
            channel.writeAndFlush( SwitchOverResponse.FAILURE );
            decline( String.format( "Attempted to switch to protocol %s version %d before negotiation complete",
                    switchOverRequest.protocolName(), switchOverRequest.version() ) );
        }
        else if ( !switchOverProtocol.get().equals( protocolStack.applicationProtocol() ) )
        {
            channel.writeAndFlush( SwitchOverResponse.FAILURE );
            decline( String.format( "Switch over mismatch: requested %s version %s but negotiated %s version %s",
                    switchOverRequest.protocolName(), switchOverRequest.version(),
                    protocolStack.applicationProtocol().category(), protocolStack.applicationProtocol().implementation() ) );
        }
        else if ( !switchOverModifiers.equals( protocolStack.modifierProtocols() ) )
        {
            channel.writeAndFlush( SwitchOverResponse.FAILURE );
            decline( String.format( "Switch over mismatch: requested modifiers %s but negotiated %s",
                    switchOverRequest.modifierProtocols(), protocolStack.modifierProtocols() ) );
        }
        else
        {
            SwitchOverResponse response = new SwitchOverResponse( SUCCESS );
            channel.writeAndFlush( response );

            protocolStackFuture.complete( protocolStack );
        }
    }

    private Set<String> supportedVersionsFor( ModifierProtocolRequest request )
    {
        return modifierProtocolRepository.supportedProtocolFor( request.protocolName() )
                .map( supported -> supported.mutuallySupportedVersionsFor( request.versions() ) )
                // else protocol has been excluded in config
                .orElse( Collections.emptySet() );
    }

    private Set<Integer> supportedVersionsFor( ApplicationProtocolRequest request )
    {
        return supportedApplicationProtocol.mutuallySupportedVersionsFor( request.versions() );
    }

    private void decline( String message )
    {
        channel.dispose();
        protocolStackFuture.completeExceptionally( new ServerHandshakeException( message, protocolStackBuilder ) );
    }

    CompletableFuture<ProtocolStack> protocolStackFuture()
    {
        return protocolStackFuture;
    }
}
