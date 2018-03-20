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
package org.neo4j.causalclustering.core;

import io.netty.channel.ChannelInboundHandler;

import java.util.Optional;

import org.neo4j.causalclustering.net.ChildInitializer;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.logging.LogProvider;

public class TransactionBackupServiceProvider
{
    private final LogProvider logProvider;
    private final LogProvider userLogProvider;
    private final TransactionBackupServiceAddressResolver transactionBackupServiceAddressResolver;
    private final ChildInitializer childInitializer;
    private final ChannelInboundHandler parentHandler;

    public TransactionBackupServiceProvider( LogProvider logProvider, LogProvider userLogProvider, ChildInitializer childInitializer,
            ChannelInboundHandler parentHandler )
    {
        this.logProvider = logProvider;
        this.userLogProvider = userLogProvider;
        this.childInitializer = childInitializer;
        this.parentHandler = parentHandler;
        this.transactionBackupServiceAddressResolver = new TransactionBackupServiceAddressResolver();
    }

    public Optional<Server> resolveIfBackupEnabled( Config config )
    {
        if ( config.get( OnlineBackupSettings.online_backup_enabled ) )
        {
            return Optional.of( new Server( childInitializer, parentHandler, logProvider, userLogProvider,
                            transactionBackupServiceAddressResolver.backupAddressForTxProtocol( config ), "backup-server" ) );
        }
        else
        {
            return Optional.empty();
        }
    }
}
