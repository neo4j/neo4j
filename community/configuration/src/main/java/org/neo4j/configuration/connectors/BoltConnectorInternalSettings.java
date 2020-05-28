/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.configuration.connectors;

import java.time.Duration;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;

@ServiceProvider
public final class BoltConnectorInternalSettings implements SettingsDeclaration
{
    @Internal
    @Description( "The queue size of the thread pool bound to this connector (-1 for unbounded, 0 for direct handoff, > 0 for bounded)" )
    public static final Setting<Integer> unsupported_thread_pool_queue_size =
            newBuilder( "dbms.connector.bolt.unsupported_thread_pool_queue_size", INT, 0 ).build();

    @Internal
    @Description( "The maximum time to wait before sending a NOOP on connections waiting for responses from active ongoing queries." )
    public static final Setting<Duration> connection_keep_alive =
            newBuilder( "dbms.connector.bolt.connection_keep_alive", DURATION, ofMinutes( 1 ) ).build();

    @Internal
    @Description( "The interval between every scheduled keep-alive check on all connections with active queries. " +
                  "Zero duration turns off keep-alive service." )
    public static final Setting<Duration> connection_keep_alive_scheduling_interval =
            newBuilder( "dbms.connector.bolt.connection_keep_alive_scheduling_interval", DURATION, ofMinutes( 0 ) ).build();

    @Internal
    @Description( "The maximum time to wait for a user to finish authentication before closing the connection." )
    public static final Setting<Duration> unsupported_bolt_unauth_connection_timeout =
            newBuilder( "dbms.connector.bolt.unsupported_unauth_connection_timeout", DURATION, ofSeconds( 30 ) ).build();

    @Internal
    @Description( "The maximum inbound message size in bytes are allowed before a connection is authenticated." )
    public static final Setting<Long> unsupported_bolt_unauth_connection_max_inbound_bytes =
            newBuilder( "dbms.connector.bolt.unsupported_unauth_max_inbound_bytes", BYTES, ByteUnit.kibiBytes( 8 ) ).build();
}
