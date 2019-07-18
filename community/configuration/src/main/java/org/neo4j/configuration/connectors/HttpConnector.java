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

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.GraphDatabaseSettings.default_advertised_address;
import static org.neo4j.configuration.GraphDatabaseSettings.default_listen_address;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;

@ServiceProvider
public class HttpConnector extends Connector
{
    public static final int DEFAULT_PORT = 7474;

    @Description( "Address the connector should bind to" )
    public final Setting<SocketAddress> listen_address =
            getBuilder( "listen_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_PORT ) ).setDependency( default_listen_address ).build();

    @Description( "Advertised address for this connector" )
    public final Setting<SocketAddress> advertised_address =
            getBuilder( "advertised_address", SOCKET_ADDRESS, new SocketAddress( DEFAULT_PORT ) ).setDependency( default_advertised_address ).build();

    public static HttpConnector group( String name )
    {
        return new HttpConnector( name );
    }

    private HttpConnector( String name )
    {
        super( name );
    }
    public HttpConnector()
    {
        super( null );  // For ServiceLoader
    }

    @Override
    public String getPrefix()
    {
        return super.getPrefix() + ".http";
    }
}
