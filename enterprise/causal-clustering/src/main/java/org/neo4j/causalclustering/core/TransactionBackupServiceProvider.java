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
package org.neo4j.causalclustering.core;

import io.netty.channel.ChannelInboundHandler;

import java.util.Collection;
import java.util.Optional;

import org.neo4j.causalclustering.catchup.CatchupServerBuilder;
import org.neo4j.causalclustering.catchup.CatchupServerHandler;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.logging.LogProvider;

public class TransactionBackupServiceProvider
{
    private final LogProvider logProvider;
    private final LogProvider userLogProvider;
    private final ChannelInboundHandler parentHandler;
    private final ApplicationSupportedProtocols catchupProtocols;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;
    private final NettyPipelineBuilderFactory serverPipelineBuilderFactory;
    private final CatchupServerHandler catchupServerHandler;

    public TransactionBackupServiceProvider( LogProvider logProvider, LogProvider userLogProvider, ApplicationSupportedProtocols catchupProtocols,
            Collection<ModifierSupportedProtocols> supportedModifierProtocols, NettyPipelineBuilderFactory serverPipelineBuilderFactory,
            CatchupServerHandler catchupServerHandler,
            ChannelInboundHandler parentHandler )
    {
        this.logProvider = logProvider;
        this.userLogProvider = userLogProvider;
        this.parentHandler = parentHandler;
        this.catchupProtocols = catchupProtocols;
        this.supportedModifierProtocols = supportedModifierProtocols;
        this.serverPipelineBuilderFactory = serverPipelineBuilderFactory;
        this.catchupServerHandler = catchupServerHandler;
    }

    public Optional<Server> resolveIfBackupEnabled( Config config )
    {
        if ( config.get( OnlineBackupSettings.online_backup_enabled ) )
        {
            ListenSocketAddress backupAddress = HostnamePortAsListenAddress.resolve( config, OnlineBackupSettings.online_backup_server );
            logProvider.getLog( TransactionBackupServiceProvider.class ).info( "Binding backup service on address %s", backupAddress );
            return Optional.of( new CatchupServerBuilder( catchupServerHandler )
                    .serverHandler( parentHandler )
                    .catchupProtocols( catchupProtocols )
                    .modifierProtocols( supportedModifierProtocols )
                    .pipelineBuilder( serverPipelineBuilderFactory )
                    .userLogProvider( userLogProvider )
                    .debugLogProvider( logProvider )
                    .listenAddress( backupAddress )
                    .serverName( "backup-server" )
                    .build());
        }
        else
        {
            return Optional.empty();
        }
    }
}
