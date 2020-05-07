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
package org.neo4j.bolt;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.provider.Arguments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.Neo4jPackV1;
import org.neo4j.bolt.packstream.Neo4jPackV2;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SecureSocketConnection;
import org.neo4j.bolt.testing.client.SecureWebSocketConnection;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.WebSocketConnection;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;

import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;

public abstract class AbstractBoltTransportsTest
{
    private static final List<Class<? extends TransportConnection>> CONNECTION_CLASSES = Arrays.asList(
            SocketConnection.class,
            WebSocketConnection.class,
            SecureSocketConnection.class,
            SecureWebSocketConnection.class );

    private static final List<Neo4jPack> NEO4J_PACK_VERSIONS = Arrays.asList(
            new Neo4jPackV1(),
            new Neo4jPackV2() );

    public Class<? extends TransportConnection> connectionClass;

    public Neo4jPack neo4jPack;

    public String name;

    protected HostnamePort address;
    protected TransportConnection connection;
    protected TransportTestUtil util;

    protected void initParameters( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        this.connectionClass = connectionClass;
        this.neo4jPack = neo4jPack;
        this.name = name;

        connection = newConnection();
        util = new TransportTestUtil( neo4jPack );
    }

    @AfterEach
    public void disconnectFromDatabase() throws Exception
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
    }

    protected static Stream<Arguments> argumentsProvider()
    {
        List<Arguments> result = new ArrayList<>();

        for ( Class<? extends TransportConnection> connectionClass : CONNECTION_CLASSES )
        {
            for ( Neo4jPack neo4jPack : NEO4J_PACK_VERSIONS )
            {
                result.add( Arguments.of( connectionClass, neo4jPack, newName( connectionClass, neo4jPack ) ) );
            }
        }
        return result.stream();
    }

    protected TransportConnection newConnection() throws Exception
    {
        return connectionClass.getDeclaredConstructor().newInstance();
    }

    protected void reconnect() throws Exception
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
        connection = newConnection();
    }

    protected Consumer<Map<Setting<?>,Object>> getSettingsFunction()
    {
        return withOptionalBoltEncryption();
    }

    private static String newName( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack )
    {
        return connectionClass.getSimpleName() + " & " + neo4jPack;
    }
}
