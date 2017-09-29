/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.backup;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.OptionalHostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

class AddressResolutionHelper
{
    HostnamePort resolveCorrectHAAddress( Config config, OptionalHostnamePort userProvidedAddress )
    {
        HostnamePort defaultValues = readDefaultConfigAddressHA( config );
        return new HostnamePort( userProvidedAddress.getHostname().orElse( defaultValues.getHost() ),
                userProvidedAddress.getPort().orElse( defaultValues.getPort() ) );
    }

    AdvertisedSocketAddress resolveCorrectCCAddress( Config config, OptionalHostnamePort userProvidedAddress )
    {
        AdvertisedSocketAddress defaultValue = readDefaultConfigAddressCC( config );
        return new AdvertisedSocketAddress( userProvidedAddress.getHostname().orElse( defaultValue.getHostname() ),
                userProvidedAddress.getPort().orElse( defaultValue.getPort() ) );
    }

    private HostnamePort readDefaultConfigAddressHA( Config config )
    {
        return OnlineBackupSettings.online_backup_server.from( config );
    }

    private AdvertisedSocketAddress readDefaultConfigAddressCC( Config config )
    {
        return advertisedFromListenAddress( CausalClusteringSettings.transaction_listen_address.from( config ) );
    }

    private AdvertisedSocketAddress advertisedFromListenAddress( ListenSocketAddress listenSocketAddress )
    {
        return new AdvertisedSocketAddress( listenSocketAddress.getHostname(), listenSocketAddress.getPort() );
    }
}
