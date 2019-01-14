/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.protocol.handshake;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.messaging.Channel;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocol;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.stream.Streams;

import static org.neo4j.causalclustering.protocol.handshake.StatusCode.SUCCESS;

public class HandshakeClient implements ClientMessageHandler
{
    private Channel channel;
    private ApplicationProtocolRepository applicationProtocolRepository;
    private ApplicationSupportedProtocols supportedApplicationProtocol;
    private ModifierProtocolRepository modifierProtocolRepository;
    private Collection<ModifierSupportedProtocols> supportedModifierProtocols;
    private ApplicationProtocol negotiatedApplicationProtocol;
    private List<Pair<String,Optional<ModifierProtocol>>> negotiatedModifierProtocols;
    private ProtocolStack protocolStack;
    private CompletableFuture<ProtocolStack> future = new CompletableFuture<>();
    private boolean magicReceived;

    public CompletableFuture<ProtocolStack> initiate( Channel channel, ApplicationProtocolRepository applicationProtocolRepository,
            ModifierProtocolRepository modifierProtocolRepository )
    {
        this.channel = channel;

        this.applicationProtocolRepository = applicationProtocolRepository;
        this.supportedApplicationProtocol = applicationProtocolRepository.supportedProtocol();

        this.modifierProtocolRepository = modifierProtocolRepository;
        this.supportedModifierProtocols = modifierProtocolRepository.supportedProtocols();

        negotiatedModifierProtocols = new ArrayList<>( supportedModifierProtocols.size() );

        channel.write( InitialMagicMessage.instance() );

        sendProtocolRequests( channel, supportedApplicationProtocol, supportedModifierProtocols );

        return future;
    }

    private void sendProtocolRequests( Channel channel, ApplicationSupportedProtocols applicationProtocols,
            Collection<ModifierSupportedProtocols> supportedModifierProtocols )
    {
        supportedModifierProtocols.forEach( modifierProtocol ->
                {
                    ProtocolSelection<String,ModifierProtocol> protocolSelection =
                            modifierProtocolRepository.getAll( modifierProtocol.identifier(), modifierProtocol.versions() );
                    channel.write( new ModifierProtocolRequest( protocolSelection.identifier(), protocolSelection.versions() ) );
                } );

        ProtocolSelection<Integer,ApplicationProtocol> applicationProtocolSelection =
                applicationProtocolRepository.getAll( applicationProtocols.identifier(), applicationProtocols.versions() );
        channel.writeAndFlush( new ApplicationProtocolRequest( applicationProtocolSelection.identifier(), applicationProtocolSelection.versions() ) );
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

        Optional<ApplicationProtocol> protocol =
                applicationProtocolRepository.select( applicationProtocolResponse.protocolName(), applicationProtocolResponse.version() );

        if ( !protocol.isPresent() )
        {
            ProtocolSelection<Integer,ApplicationProtocol> knownApplicationProtocolVersions =
                    applicationProtocolRepository.getAll( supportedApplicationProtocol.identifier(), supportedApplicationProtocol.versions() );
            decline( String.format(
                    "Mismatch of application protocols between client and server: Server protocol %s version %d: Client protocol %s versions %s",
                    applicationProtocolResponse.protocolName(), applicationProtocolResponse.version(),
                    knownApplicationProtocolVersions.identifier(), knownApplicationProtocolVersions.versions() ) );
        }
        else
        {
            negotiatedApplicationProtocol = protocol.get();

            sendSwitchOverRequestIfReady();
        }
    }

    @Override
    public void handle( ModifierProtocolResponse modifierProtocolResponse )
    {
        ensureMagic();
        if ( modifierProtocolResponse.statusCode() == StatusCode.SUCCESS )
        {
            Optional<ModifierProtocol> selectedModifierProtocol =
                    modifierProtocolRepository.select( modifierProtocolResponse.protocolName(), modifierProtocolResponse.version() );
            negotiatedModifierProtocols.add( Pair.of( modifierProtocolResponse.protocolName(), selectedModifierProtocol ) );
        }
        else
        {
            negotiatedModifierProtocols.add( Pair.of( modifierProtocolResponse.protocolName(), Optional.empty() ) );
        }

        sendSwitchOverRequestIfReady();
    }

    private void sendSwitchOverRequestIfReady()
    {
        if ( negotiatedApplicationProtocol != null && negotiatedModifierProtocols.size() == supportedModifierProtocols.size() )
        {
            List<ModifierProtocol> agreedModifierProtocols = negotiatedModifierProtocols
                    .stream()
                    .map( Pair::other )
                    .flatMap( Streams::ofOptional )
                    .collect( Collectors.toList() );

            protocolStack = new ProtocolStack( negotiatedApplicationProtocol, agreedModifierProtocols );
            List<Pair<String,String>> switchOverModifierProtocols =
                    agreedModifierProtocols
                            .stream()
                            .map( protocol -> Pair.of( protocol.category(), protocol.implementation() ) )
                            .collect( Collectors.toList() );

            channel.writeAndFlush(
                    new SwitchOverRequest(
                            negotiatedApplicationProtocol.category(),
                            negotiatedApplicationProtocol.implementation(),
                            switchOverModifierProtocols ) );
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
        future.completeExceptionally( new ClientHandshakeException( message, negotiatedApplicationProtocol, negotiatedModifierProtocols ) );
    }
}
