/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.transport.socket.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import org.neo4j.function.Factory;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.ndp.transport.socket.client.Connection;
import org.neo4j.ndp.transport.socket.client.SocketConnection;
import org.neo4j.ndp.transport.socket.client.WebSocketConnection;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.ndp.messaging.v1.message.Messages.initialize;
import static org.neo4j.ndp.messaging.v1.message.Messages.pullAll;
import static org.neo4j.ndp.messaging.v1.message.Messages.run;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.msgRecord;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.msgSuccess;
import static org.neo4j.ndp.transport.socket.integration.TransportTestUtil.acceptedVersions;
import static org.neo4j.ndp.transport.socket.integration.TransportTestUtil.chunk;
import static org.neo4j.ndp.transport.socket.integration.TransportTestUtil.eventuallyRecieves;
import static org.neo4j.runtime.internal.runner.StreamMatchers.eqRecord;

@RunWith(Parameterized.class)
public class TransportSessionIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket();

    @Parameterized.Parameter(0)
    public Factory<Connection> cf;

    @Parameterized.Parameter(1)
    public HostnamePort address;

    private Connection client;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return asList(
                new Object[]{
                        new Factory<Connection>()
                        {
                            @Override
                            public Connection newInstance()
                            {
                                return new SocketConnection();
                            }
                        },
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        new Factory<Connection>()
                        {
                            @Override
                            public Connection newInstance()
                            {
                                return new WebSocketConnection();
                            }
                        },
                        new HostnamePort( "localhost:7688" )
                } );
    }

    @Test
    public void shouldNegotiateProtocolVersion() throws Throwable
    {
        // When
        client.connect( address )
                .send( acceptedVersions( 1, 0, 0, 0 ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
    }

    @Test
    public void shouldReturnNilOnNoApplicableVersion() throws Throwable
    {
        // When
        client.connect( address )
                .send( acceptedVersions( 1337, 0, 0, 0 ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 0} ) );
    }

    @Test
    public void shouldRunSimpleStatement() throws Throwable
    {
        // When
        client.connect( address )
                .send( acceptedVersions( 1, 0, 0, 0 ) )
                .send( chunk(
                        initialize("TestClient/1.1"),
                        run( "UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared" ),
                        pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves(
                msgSuccess(),
                msgSuccess( map( "fields", asList( "a", "a_squared" ) ) ),
                msgRecord( eqRecord( equalTo( 1l ), equalTo( 1l ) ) ),
                msgRecord( eqRecord( equalTo( 2l ), equalTo( 4l ) ) ),
                msgRecord( eqRecord( equalTo( 3l ), equalTo( 9l ) ) ),
                msgSuccess() ) );
    }

    @Before
    public void setup()
    {
        this.client = cf.newInstance();
    }

    @After
    public void teardown() throws Exception
    {
        if(client != null) client.disconnect();
    }

}
