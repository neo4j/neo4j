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
package org.neo4j.backup.impl;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;

class AddressResolver
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
        return config.get( OnlineBackupSettings.online_backup_server );
    }

    private AdvertisedSocketAddress readDefaultConfigAddressCC( Config config )
    {
        return asAdvertised( config.get( OnlineBackupSettings.online_backup_server ) );
    }

    private AdvertisedSocketAddress asAdvertised( HostnamePort listenSocketAddress )
    {
        return new AdvertisedSocketAddress( listenSocketAddress.getHost(), listenSocketAddress.getPort() );
    }
}
