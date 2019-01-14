/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.advertisedAddress;
import static org.neo4j.kernel.configuration.Settings.legacyFallback;
import static org.neo4j.kernel.configuration.Settings.listenAddress;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.setting;

@Description( "Configuration options for HTTP connectors. " +
        "\"(http-connector-key)\" is a placeholder for a unique name for the connector, for instance " +
        "\"http-public\" or some other name that describes what the connector is for." )
public class HttpConnector extends Connector
{
    @Description( "Enable TLS for this connector" )
    public final Setting<Encryption> encryption;

    @Description( "Address the connector should bind to. " +
            "This setting is deprecated and will be replaced by `+listen_address+`" )
    @Deprecated
    @ReplacedBy( "dbms.connector.X.listen_address" )
    public final Setting<ListenSocketAddress> address;

    @Description( "Address the connector should bind to" )
    public final Setting<ListenSocketAddress> listen_address;

    @Description( "Advertised address for this connector" )
    public final Setting<AdvertisedSocketAddress> advertised_address;
    private final Encryption encryptionLevel;

    // Used by config doc generator
    public HttpConnector()
    {
        this( "(http-connector-key)" );
    }

    public HttpConnector( Encryption encryptionLevel )
    {
        this( "(http-connector-key)", encryptionLevel );
    }

    public HttpConnector( String key )
    {
        this( key, Encryption.NONE );
    }

    public HttpConnector( String key, Encryption encryptionLevel )
    {
        super( key );
        this.encryptionLevel = encryptionLevel;
        encryption = group.scope( setting( "encryption", options( HttpConnector.Encryption.class ), NO_DEFAULT ) );
        Setting<ListenSocketAddress> legacyAddressSetting = listenAddress( "address", encryptionLevel.defaultPort );
        Setting<ListenSocketAddress> listenAddressSetting = legacyFallback( legacyAddressSetting,
                listenAddress( "listen_address", encryptionLevel.defaultPort ) );

        this.address = group.scope( legacyAddressSetting );
        this.listen_address = group.scope( listenAddressSetting );
        this.advertised_address = group.scope( advertisedAddress( "advertised_address", listenAddressSetting ) );
    }

    /**
     * @return this connector's configured encryption level
     */
    public Encryption encryptionLevel()
    {
        return encryptionLevel;
    }

    public enum Encryption
    {
        NONE( "http", 7474 ), TLS( "https", 7473 );

        public final String uriScheme;
        public final int defaultPort;

        Encryption( String uriScheme, int defaultPort )
        {
            this.uriScheme = uriScheme;
            this.defaultPort = defaultPort;
        }
    }
}
