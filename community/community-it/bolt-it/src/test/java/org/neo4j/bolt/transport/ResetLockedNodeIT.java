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
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.begin;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.commit;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.goodbye;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.hello;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.pull;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.reset;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.run;
import static org.neo4j.values.storable.Values.intValue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.values.virtual.MapValueBuilder;

@TestDirectoryExtension
@Neo4jWithSocketExtension
public class ResetLockedNodeIT {

    private HostnamePort serverAddress;

    @Inject
    public Neo4jWithSocket server;

    @BeforeEach
    void setUp(TestInfo testInfo) throws Exception {
        server.setGraphDatabaseFactory(new TestDatabaseManagementServiceBuilder());
        server.setConfigure(settings -> {
            settings.put(BoltConnector.enabled, true);
            settings.put(BoltConnector.listen_address, new SocketAddress(0));
        });
        server.init(testInfo);

        serverAddress = server.lookupConnector(ConnectorType.BOLT);

        SocketConnection connection = new SocketConnection(serverAddress);
        initializeConnection(connection);

        connection.send(run("CREATE (n {id: 123})")).send(pull(100));

        assertThat(connection).receivesSuccess(2);

        connection.send(goodbye());
    }

    @Test
    @Timeout(15)
    void shouldErrorWhenResettingAConnectionWaitingOnALock() throws Exception {
        SocketConnection connA = new SocketConnection(serverAddress);
        SocketConnection connB = new SocketConnection(serverAddress);

        initializeConnection(connA);
        initializeConnection(connB);

        var paramsA = new MapValueBuilder();
        paramsA.add("currentId", intValue(123));
        paramsA.add("newId", intValue(456));

        var paramsB = new MapValueBuilder();
        paramsB.add("currentId", intValue(123));
        paramsB.add("newId", intValue(789));

        // Given a connectionA has acquired a lock whilst updating a node.
        connA.send(begin())
                .send(run("MATCH (n {id: $currentId}) SET n.id = $newId", paramsA.build()))
                .send(pull(100));

        assertThat(connA).receivesSuccess(3);

        // And when connectionB is blocked waiting to acquire the same lock.
        connB.send(run("MATCH (n {id: $currentId}) SET n.id = $newId", paramsB.build()))
                .send(pull(100));

        Thread.sleep(300); // Allow time for connection B to become blocked on a lock.

        // When the connectionB is RESET
        connB.send(reset());

        // Then that connection receives an error
        assertThat(connB)
                .receivesFailure(Status.Transaction.LockClientStopped)
                .receivesIgnored()
                .receivesSuccess();

        // And the other connection can commit successfully.
        connA.send(commit());

        assertThat(connA).receivesSuccess();
    }

    private void initializeConnection(SocketConnection connection) throws Exception {
        connection.connect().sendDefaultProtocolVersion().send(hello());

        assertThat(connection).negotiatesDefaultVersion().receivesSuccess();
    }
}
