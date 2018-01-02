/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.configuration;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.ReplacedBy;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;

import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.kernel.configuration.Settings.advertisedAddress;
import static org.neo4j.kernel.configuration.Settings.legacyFallback;
import static org.neo4j.kernel.configuration.Settings.listenAddress;
import static org.neo4j.kernel.configuration.Settings.options;

@Description( "Configuration options for Bolt connectors. " +
              "\"(bolt-connector-key)\" is a placeholder for a unique name for the connector, for instance " +
              "\"bolt-public\" or some other name that describes what the connector is for." )
public class BoltConnector extends Connector
{
    @Description( "Encryption level to require this connector to use" )
    public final Setting<EncryptionLevel> encryption_level;

    @Description( "Address the connector should bind to. " +
            "This setting is deprecated and will be replaced by `+listen_address+`" )
    @Deprecated
    @ReplacedBy( "dbms.connector.X.listen_address" )
    public final Setting<ListenSocketAddress> address;

    @Description( "Address the connector should bind to" )
    public final Setting<ListenSocketAddress> listen_address;

    @Description( "Advertised address for this connector" )
    public final Setting<AdvertisedSocketAddress> advertised_address;

    // Used by config doc generator
    public BoltConnector()
    {
        this( "(bolt-connector-key)" );
    }

    public BoltConnector( String key )
    {
        super( key );
        encryption_level = group.scope(
                Settings.setting( "tls_level", options( EncryptionLevel.class ), OPTIONAL.name() ) );
        Setting<ListenSocketAddress> legacyAddressSetting = listenAddress( "address", 7687 );
        Setting<ListenSocketAddress> listenAddressSetting = legacyFallback( legacyAddressSetting,
                listenAddress( "listen_address", 7687 ) );

        this.address = group.scope( legacyAddressSetting );
        this.listen_address = group.scope( listenAddressSetting );
        this.advertised_address = group.scope( advertisedAddress( "advertised_address", listenAddressSetting ) );
    }

    public enum EncryptionLevel
    {
        REQUIRED,
        OPTIONAL,
        DISABLED
    }
}
