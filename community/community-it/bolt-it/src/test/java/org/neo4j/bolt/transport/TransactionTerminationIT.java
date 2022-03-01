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
package org.neo4j.bolt.transport;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.util.stream.Collectors;

import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.msgFailure;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyReceivesSelectedProtocolVersion;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.newMessageEncoder;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

@TestDirectoryExtension
@Neo4jWithSocketExtension
public class TransactionTerminationIT
{

    private TransportTestUtil util;
    private HostnamePort serverAddress;

    @Inject
    public Neo4jWithSocket server;

    @BeforeEach
    void setUp( TestInfo testInfo ) throws Exception
    {
        var test = new TestDatabaseManagementServiceBuilder();
        server.setGraphDatabaseFactory( test );
        server.setConfigure( settings ->
                             {
                                 settings.put( BoltConnector.enabled, true );
                                 settings.put( BoltConnector.listen_address, new SocketAddress( 0 ) );
                             } );
        server.init( testInfo );

        serverAddress = server.lookupConnector( BoltConnector.NAME );
        util = new TransportTestUtil( newMessageEncoder() );
    }

    @Test
    @Timeout( 15 )
    void killTxViaReset() throws Exception
    {
        SocketConnection connA = initializeConnection( serverAddress );

        connA.send( util.chunk( new BeginMessage() ) );
        connA.send( util.chunk( new RunMessage( "UNWIND range(1, 2000000) AS i CREATE (n)" ) ) );

        //let the query start actually executing
        awaitTransactionStart();

        connA.send( util.chunk( ResetMessage.INSTANCE ) );

        assertThat( connA ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        assertThat( connA ).satisfies( util.eventuallyReceives( msgFailure( Status.Transaction.Terminated ) ) );
        assertThat( connA ).satisfies( util.eventuallyReceives( msgSuccess() ) );
    }

    public void awaitTransactionStart() throws InterruptedException
    {
        long txCount = 1;
        while ( txCount <= 1 )
        {
            var tx = server.graphDatabaseService().beginTx();
            var result = tx.execute( "SHOW TRANSACTIONS" );
            txCount = result.stream().collect( Collectors.toList() ).size();
            System.out.println( txCount );
            tx.close();
            Thread.sleep( 100 );
        }
    }

    private SocketConnection initializeConnection( HostnamePort address ) throws Exception
    {
        SocketConnection socketConnection = new SocketConnection();

        socketConnection.connect( address ).send( TransportTestUtil.defaultAcceptedVersions() );
        assertThat( socketConnection ).satisfies( eventuallyReceivesSelectedProtocolVersion() );

        socketConnection.send( util.chunk( new HelloMessage( map( "user_agent", "TESTCLIENT/4.2" ) ) ) );
        assertThat( socketConnection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

        return socketConnection;
    }
}
