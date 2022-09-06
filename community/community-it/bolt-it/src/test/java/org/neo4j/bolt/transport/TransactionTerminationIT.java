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

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltDefaultWire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

@TestDirectoryExtension
@Neo4jWithSocketExtension
public class TransactionTerminationIT {

    private HostnamePort serverAddress;

    @Inject
    public Neo4jWithSocket server;

    private final BoltWire wire = new BoltDefaultWire();

    @BeforeEach
    void setUp(TestInfo testInfo) throws Exception {
        var test = new TestDatabaseManagementServiceBuilder();
        server.setGraphDatabaseFactory(test);
        server.setConfigure(settings -> {
            settings.put(BoltConnector.enabled, true);
            settings.put(BoltConnector.listen_address, new SocketAddress(0));
        });
        server.init(testInfo);

        serverAddress = server.lookupConnector(ConnectorType.BOLT);
    }

    @Test
    @Timeout(15)
    void killTxViaReset() throws Exception {
        var connection = initializeConnection(serverAddress);

        connection.send(wire.begin()).send(wire.run("UNWIND range(1, 2000000) AS i CREATE (n)"));

        // let the query start actually executing
        awaitTransactionStart();

        connection.send(wire.reset());

        assertThat(connection)
                .receivesSuccess()
                .receivesFailure(Status.Transaction.Terminated, Status.Transaction.LockClientStopped)
                .receivesSuccess();
    }

    public void awaitTransactionStart() throws InterruptedException {
        long txCount = 1;
        while (txCount <= 1) {
            var tx = server.graphDatabaseService().beginTx();
            var result = tx.execute("SHOW TRANSACTIONS");
            txCount = result.stream().toList().size();
            tx.close();
            Thread.sleep(100);
        }
    }

    private TransportConnection initializeConnection(HostnamePort address) throws Exception {
        var connection = new SocketConnection(address)
                .connect()
                .sendDefaultProtocolVersion()
                .send(wire.hello());

        assertThat(connection).negotiatesDefaultVersion().receivesSuccess();

        return connection;
    }
}
