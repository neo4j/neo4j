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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.msgFailure;
import static org.neo4j.bolt.testing.MessageConditions.msgIgnored;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyReceivesSelectedProtocolVersion;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.newMessageEncoder;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.storable.Values.intValue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.CommitMessage;
import org.neo4j.bolt.v3.messaging.request.GoodbyeMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.bolt.v4.messaging.PullMessage;
import org.neo4j.configuration.connectors.BoltConnector;
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

    private TransportTestUtil util;
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

        serverAddress = server.lookupConnector(BoltConnector.NAME);
        util = new TransportTestUtil(newMessageEncoder());

        SocketConnection connection = new SocketConnection();
        initializeConnection(connection, serverAddress);
        connection.send(util.chunk(new RunMessage("CREATE (n {id: 123})")));
        connection.send(util.chunk(new PullMessage(asMapValue(map("n", 100L)))));
        assertThat(connection).satisfies(util.eventuallyReceives(msgSuccess()));
        assertThat(connection).satisfies(util.eventuallyReceives(msgSuccess()));
        connection.send(util.chunk(GoodbyeMessage.GOODBYE_MESSAGE));
    }

    @Test
    @Timeout(15)
    void shouldErrorWhenResettingAConnectionWaitingOnALock() throws Exception {
        SocketConnection connA = new SocketConnection();
        SocketConnection connB = new SocketConnection();

        initializeConnection(connA, serverAddress);
        initializeConnection(connB, serverAddress);

        // Given a connectionA has acquired a lock whilst updating a node.
        connA.send(util.chunk(new BeginMessage()));
        MapValueBuilder parameterValue = new MapValueBuilder();
        parameterValue.add("currentId", intValue(123));
        parameterValue.add("newId", intValue(456));
        connA.send(util.chunk(new RunMessage("MATCH (n {id: $currentId}) SET n.id = $newId", parameterValue.build())));
        connA.send(util.chunk(new PullMessage(asMapValue(map("n", 100L)))));
        assertThat(connA).satisfies(util.eventuallyReceives(msgSuccess()));
        assertThat(connA).satisfies(util.eventuallyReceives(msgSuccess()));
        assertThat(connA).satisfies(util.eventuallyReceives(msgSuccess()));

        // And when connectionB is blocked waiting to acquire the same lock.
        MapValueBuilder parameterValueB = new MapValueBuilder();
        parameterValueB.add("currentId", intValue(123));
        parameterValueB.add("newId", intValue(789));
        connB.send(util.chunk(
                new RunMessage("MATCH (n {id: $currentId}) SET n.id = $newId", parameterValueB.build()),
                new PullMessage(asMapValue(map("n", 100L)))));
        Thread.sleep(300); // Allow time for connection B to become blocked on a lock.

        // When the connectionB is RESET
        connB.send(util.chunk(ResetMessage.INSTANCE));

        // Then that connection receives an error
        assertThat(connB).satisfies(util.eventuallyReceives(msgFailure(Status.Transaction.LockClientStopped)));
        assertThat(connB).satisfies(util.eventuallyReceives(msgIgnored()));
        assertThat(connB).satisfies(util.eventuallyReceives(msgSuccess()));

        // And the other connection can commit successfully.
        connA.send(util.chunk(CommitMessage.COMMIT_MESSAGE));
        assertThat(connA).satisfies(util.eventuallyReceives(msgSuccess()));
    }

    private void initializeConnection(SocketConnection connection, HostnamePort address) throws Exception {
        connection.connect(address).send(TransportTestUtil.defaultAcceptedVersions());
        assertThat(connection).satisfies(eventuallyReceivesSelectedProtocolVersion());

        connection.send(util.chunk(new HelloMessage(map("user_agent", "TESTCLIENT/4.2"))));
        assertThat(connection).satisfies(util.eventuallyReceives(msgSuccess()));
    }
}
