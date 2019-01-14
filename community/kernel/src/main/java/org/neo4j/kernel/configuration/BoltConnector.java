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

import java.time.Duration;

import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.ReplacedBy;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;

import static org.neo4j.kernel.configuration.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.advertisedAddress;
import static org.neo4j.kernel.configuration.Settings.legacyFallback;
import static org.neo4j.kernel.configuration.Settings.listenAddress;
import static org.neo4j.kernel.configuration.Settings.options;
import static org.neo4j.kernel.configuration.Settings.setting;

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

    @Description( "The number of threads to keep in the thread pool bound to this connector, even if they are idle." )
    public final Setting<Integer> thread_pool_min_size;

    @Description( "The maximum number of threads allowed in the thread pool bound to this connector." )
    public final Setting<Integer> thread_pool_max_size;

    @Description( "The maximum time an idle thread in the thread pool bound to this connector will wait for new tasks." )
    public final Setting<Duration> thread_pool_keep_alive;

    @Description( "The queue size of the thread pool bound to this connector (-1 for unbounded, 0 for direct handoff, > 0 for bounded)" )
    @Internal
    public final Setting<Integer> unsupported_thread_pool_queue_size;

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
        this.thread_pool_min_size = group.scope( setting( "thread_pool_min_size", INTEGER, String.valueOf( 5 ) ) );
        this.thread_pool_max_size = group.scope( setting( "thread_pool_max_size", INTEGER, String.valueOf( 400 ) ) );
        this.thread_pool_keep_alive = group.scope( setting( "thread_pool_keep_alive", DURATION, "5m" ) );
        this.unsupported_thread_pool_queue_size = group.scope( setting( "unsupported_thread_pool_queue_size", INTEGER, String.valueOf( 0 ) ) );
    }

    public enum EncryptionLevel
    {
        REQUIRED,
        OPTIONAL,
        DISABLED
    }
}
