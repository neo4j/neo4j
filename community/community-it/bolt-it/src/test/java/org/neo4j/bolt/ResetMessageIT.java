/*
 * Copyright (c) "Neo4j"
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v4.messaging.BeginMessage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.newMessageEncoder;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@ExtendWith( OtherThreadExtension.class )
public class ResetMessageIT
{
    private static final String USER_AGENT = "TestClient/4.0";

    @Inject
    public Neo4jWithSocket server;

    private HostnamePort address;
    private TransportConnection connection;
    private TransportTestUtil util;

    private static Stream<byte[]> versionProvider()
    {
        return Stream.of(
                TransportTestUtil.acceptedVersions( 4, 0, 0, 0 ),
                TransportTestUtil.acceptedVersions( 4, 1, 0, 0 ),
                TransportTestUtil.acceptedVersions( 4, 2, 0, 0 ),
                TransportTestUtil.acceptedVersions( 4, 3, 0, 0 ),
                TransportTestUtil.acceptedVersions( 4, 4, 0, 0 ),
                TransportTestUtil.acceptedVersions( 3, 0, 0, 0 ));
    }

    @BeforeEach
    public void setup( TestInfo testInfo ) throws Exception
    {
        server.setGraphDatabaseFactory( new TestDatabaseManagementServiceBuilder() );
        server.setConfigure( withOptionalBoltEncryption() );
        server.init( testInfo );

        address = server.lookupDefaultConnector();
        connection = new SocketConnection();
        util = new TransportTestUtil( newMessageEncoder() );

        GraphDatabaseService gds = server.graphDatabaseService();
        try ( Transaction tx = gds.beginTx() )
        {
            for ( int i = 30; i <= 40; i++ )
            {
                tx.createNode( Label.label( "L" + i ) );
            }
            tx.commit();
        }
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "versionProvider" )
    void shouldResetToReadyStateWhenAuthenticated( byte[] acceptedVersion )
            throws Exception
    {
        connection.connect( address ).send( acceptedVersion );
        assertNotNull(connection.recv(4));

        connection.send( util.chunk( new HelloMessage( MapUtil.map( "user_agent", USER_AGENT ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( new BeginMessage() ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk(ResetMessage.INSTANCE) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        connection.send( util.chunk( new BeginMessage() ) );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "versionProvider" )
    void shouldFailAResetWhenInUnauthenticatedStateLegacy( byte[] acceptedVersion )
            throws Exception
    {
        connection.connect( address ).send( acceptedVersion );
        assertNotNull(connection.recv(4));

        connection.send( util.chunk(ResetMessage.INSTANCE) );
        assertThat(connection).satisfies( eventuallyDisconnects() );
    }
}
