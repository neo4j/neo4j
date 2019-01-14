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
package org.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation.Client;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import org.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory.VOID_WRAPPER;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.CATCHUP;
import static org.neo4j.time.Clocks.systemClock;

public class CatchupClientBuilder
{
    private Duration handshakeTimeout = Duration.ofSeconds( 5 );
    private LogProvider logProvider = NullLogProvider.getInstance();
    private NettyPipelineBuilderFactory pipelineBuilder = new NettyPipelineBuilderFactory( VOID_WRAPPER );
    private ApplicationSupportedProtocols catchupProtocols = new ApplicationSupportedProtocols( CATCHUP, emptyList() );
    private Collection<ModifierSupportedProtocols> modifierProtocols = emptyList();
    private long inactivityTimeoutMillis = SECONDS.toMillis( 20 );
    private Clock clock = systemClock();

    public CatchupClientBuilder()
    {
    }

    public CatchupClientBuilder( ApplicationSupportedProtocols catchupProtocols, Collection<ModifierSupportedProtocols> modifierProtocols,
            NettyPipelineBuilderFactory pipelineBuilder, Duration handshakeTimeout, long inactivityTimeoutMillis, LogProvider logProvider, Clock clock )
    {
        this.catchupProtocols = catchupProtocols;
        this.modifierProtocols = modifierProtocols;
        this.pipelineBuilder = pipelineBuilder;
        this.handshakeTimeout = handshakeTimeout;
        this.logProvider = logProvider;
        this.inactivityTimeoutMillis = inactivityTimeoutMillis;
        this.clock = clock;
    }

    public CatchupClientBuilder catchupProtocols( ApplicationSupportedProtocols catchupProtocols )
    {
        this.catchupProtocols = catchupProtocols;
        return this;
    }

    public CatchupClientBuilder modifierProtocols( Collection<ModifierSupportedProtocols> modifierProtocols )
    {
        this.modifierProtocols = modifierProtocols;
        return this;
    }

    public CatchupClientBuilder pipelineBuilder( NettyPipelineBuilderFactory pipelineBuilder )
    {
        this.pipelineBuilder = pipelineBuilder;
        return this;
    }

    public CatchupClientBuilder handshakeTimeout( Duration handshakeTimeout )
    {
        this.handshakeTimeout = handshakeTimeout;
        return this;
    }

    public CatchupClientBuilder inactivityTimeoutMillis( long inactivityTimeoutMillis )
    {
        this.inactivityTimeoutMillis = inactivityTimeoutMillis;
        return this;
    }

    public CatchupClientBuilder logProvider( LogProvider logProvider )
    {
        this.logProvider = logProvider;
        return this;
    }

    public CatchupClientBuilder clock( Clock clock )
    {
        this.clock = clock;
        return this;
    }

    public CatchUpClient build()
    {
        ApplicationProtocolRepository applicationProtocolRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), catchupProtocols );
        ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( ModifierProtocols.values(), modifierProtocols );

        Function<CatchUpResponseHandler,ChannelInitializer<SocketChannel>> channelInitializer = handler -> {
            List<ProtocolInstaller.Factory<Client,?>> installers = singletonList(
                    new CatchupProtocolClientInstaller.Factory( pipelineBuilder, logProvider, handler ) );

            ProtocolInstallerRepository<Client> protocolInstallerRepository = new ProtocolInstallerRepository<>( installers,
                    ModifierProtocolInstaller.allClientInstallers );

            return new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository, protocolInstallerRepository, pipelineBuilder,
                    handshakeTimeout, logProvider );
        };

        return new CatchUpClient( logProvider, clock, inactivityTimeoutMillis, channelInitializer );
    }
}
