/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
