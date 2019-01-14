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
package org.neo4j.bolt;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.helpers.HostnamePort;

import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith( Parameterized.class )
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

    @Parameter( 0 )
    public Class<? extends TransportConnection> connectionClass;

    @Parameter( 1 )
    public Neo4jPack neo4jPack;

    @Parameter( 2 )
    public String name;

    protected HostnamePort address;
    protected TransportConnection connection;
    protected TransportTestUtil util;

    @Before
    public void initializeConnectionAndUtil() throws Exception
    {
        connection = connectionClass.newInstance();
        util = new TransportTestUtil( neo4jPack );
    }

    @After
    public void disconnectFromDatabase() throws Exception
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
    }

    @Parameters( name = "{2}" )
    public static List<Object[]> parameters()
    {
        List<Object[]> result = new ArrayList<>();
        for ( Class<? extends TransportConnection> connectionClass : CONNECTION_CLASSES )
        {
            for ( Neo4jPack neo4jPack : NEO4J_PACK_VERSIONS )
            {
                result.add( new Object[]{connectionClass, neo4jPack, newName( connectionClass, neo4jPack )} );
            }
        }
        return result;
    }

    protected TransportConnection newConnection() throws Exception
    {
        return connectionClass.newInstance();
    }

    protected void reconnect() throws Exception
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
        connection = newConnection();
    }

    private static String newName( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack )
    {
        return connectionClass.getSimpleName() + " & " + neo4jPack;
    }
}
