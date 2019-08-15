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
package org.neo4j.configuration.connectors;

import java.time.Duration;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.neo4j.configuration.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.configuration.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.DISABLED;

@ServiceProvider
@PublicApi
public final class BoltConnector implements SettingsDeclaration
{
    public static final int DEFAULT_PORT = 7687;
    public static final String NAME = "bolt";

    public static final Setting<Boolean> enabled = newBuilder( "dbms.connector.bolt.enabled", BOOL, false ).build();

    @Description( "Encryption level to require this connector to use" )
    public static final Setting<EncryptionLevel> encryption_level =
            newBuilder( "dbms.connector.bolt.tls_level", ofEnum( EncryptionLevel.class ), DISABLED ).build();

    @Description( "Address the connector should bind to" )
    public static final Setting<SocketAddress> listen_address =
            newBuilder( "dbms.connector.bolt.listen_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_PORT ) )
                    .setDependency( default_listen_address )
                    .build();

    @Description( "Advertised address for this connector" )
    public static final Setting<SocketAddress> advertised_address =
            newBuilder( "dbms.connector.bolt.advertised_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_PORT ) )
                    .setDependency( default_advertised_address )
                    .build();

    @Description( "The number of threads to keep in the thread pool bound to this connector, even if they are idle." )
    public static final Setting<Integer> thread_pool_min_size = newBuilder( "dbms.connector.bolt.thread_pool_min_size", INT, 5 ).build();

    @Description( "The maximum number of threads allowed in the thread pool bound to this connector." )
    public static final Setting<Integer> thread_pool_max_size = newBuilder( "dbms.connector.bolt.thread_pool_max_size", INT, 400 ).build();

    @Description( "The maximum time an idle thread in the thread pool bound to this connector will wait for new tasks." )
    public static final Setting<Duration> thread_pool_keep_alive =
            newBuilder( "dbms.connector.bolt.thread_pool_keep_alive", DURATION, ofMinutes( 5 ) ).build();

    @Description( "The maximum time to wait for the thread pool to finish processing its pending jobs and shutdown" )
    public static final Setting<Duration> thread_pool_shutdown_wait_time =
            newBuilder( "dbms.connector.bolt.unsupported_thread_pool_shutdown_wait_time", DURATION, ofSeconds( 5 ) ).build();

    @Description( "The queue size of the thread pool bound to this connector (-1 for unbounded, 0 for direct handoff, > 0 for bounded)" )
    @Internal
    public static final Setting<Integer> unsupported_thread_pool_queue_size =
            newBuilder( "dbms.connector.bolt.unsupported_thread_pool_queue_size", INT, 0 ).build();

    public enum EncryptionLevel
    {
        REQUIRED,
        OPTIONAL,
        DISABLED
    }
}
