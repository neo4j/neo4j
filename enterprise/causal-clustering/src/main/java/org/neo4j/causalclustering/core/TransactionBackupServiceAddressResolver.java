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

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.SocketAddressParser;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

class TransactionBackupServiceAddressResolver
{
    ListenSocketAddress backupAddressForTxProtocol( Config config )
    {
        // We cannot use the backup address setting directly as IPv6 isn't processed during config read
        String settingName = OnlineBackupSettings.online_backup_server.name();
        HostnamePort resolvedValueFromConfig = resolved( config );
        String modifiedLiteralValueToAvoidRange = String.format( "%s:%d", resolvedValueFromConfig.getHost(), resolvedValueFromConfig.getPort() );
        String defaultHostname = resolvedValueFromConfig.getHost();
        AdvertisedSocketAddress advertisedSocketAddress =
                SocketAddressParser.deriveSocketAddress( settingName, modifiedLiteralValueToAvoidRange, defaultHostname, resolvedValueFromConfig.getPort(),
                        AdvertisedSocketAddress::new );
        return new ListenSocketAddress( advertisedSocketAddress.getHostname(), advertisedSocketAddress.getPort() );
    }

    private HostnamePort resolved( Config config )
    {
        int defaultPort = Config.defaults().get( OnlineBackupSettings.online_backup_server ).getPort();
        HostnamePort resolved = config.get( OnlineBackupSettings.online_backup_server );
        if ( resolved.getPort() == 0 ) // Was the port not specified by user?
        {
            resolved = new HostnamePort( resolved.getHost(), defaultPort );
        }
        return resolved;
    }
}
