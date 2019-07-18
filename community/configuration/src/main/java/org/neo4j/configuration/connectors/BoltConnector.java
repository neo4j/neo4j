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

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;

import static java.time.Duration.ofMinutes;
import static org.neo4j.configuration.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.configuration.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;

@ServiceProvider
public class BoltConnector extends Connector
{
    public static final int DEFAULT_PORT = 7687;

    @Description( "Encryption level to require this connector to use" )
    public Setting<EncryptionLevel> encryption_level =
            getBuilder( "encryption_level", ofEnum( EncryptionLevel.class ), EncryptionLevel.OPTIONAL ).build();

    @Description( "Address the connector should bind to" )
    public final Setting<SocketAddress> listen_address =
            getBuilder( "listen_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_PORT ) ).setDependency( default_listen_address ).build();

    @Description( "Advertised address for this connector" )
    public final Setting<SocketAddress> advertised_address =
            getBuilder( "advertised_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_PORT ) ).setDependency( default_advertised_address ).build();

    @Description( "The number of threads to keep in the thread pool bound to this connector, even if they are idle." )
    public final Setting<Integer> thread_pool_min_size = getBuilder( "thread_pool_min_size", INT, 5 ).build();

    @Description( "The maximum number of threads allowed in the thread pool bound to this connector." )
    public final Setting<Integer> thread_pool_max_size = getBuilder( "thread_pool_max_size", INT, 400 ).build();

    @Description( "The maximum time an idle thread in the thread pool bound to this connector will wait for new tasks." )
    public final Setting<Duration> thread_pool_keep_alive = getBuilder( "thread_pool_keep_alive", DURATION, ofMinutes( 5 ) ).build() ;

    @Description( "The queue size of the thread pool bound to this connector (-1 for unbounded, 0 for direct handoff, > 0 for bounded)" )
    @Internal
    public final Setting<Integer> unsupported_thread_pool_queue_size = getBuilder( "unsupported_thread_pool_queue_size", INT, 0 ).build();

    public static BoltConnector group( String name )
    {
        return new BoltConnector( name );
    }

    private BoltConnector( String name )
    {
        super( name );
    }
    public BoltConnector()
    {
        super( null ); // For ServiceLoader
    }

    @Override
    public String getPrefix()
    {
        return super.getPrefix() + ".bolt";
    }

    public enum EncryptionLevel
    {
        REQUIRED,
        OPTIONAL,
        DISABLED
    }
}
