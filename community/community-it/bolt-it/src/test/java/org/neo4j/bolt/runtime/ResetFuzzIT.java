/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.runtime;

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltDefaultWire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.testing.sequence.RequestSequenceCollection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.SpiedAssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@Disabled("Disabled since 2022-05-30. As of 2023-02-03 there are plans to reevaluating this test.")
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@ExtendWith(SuppressOutputExtension.class)
@ResourceLock(Resources.SYSTEM_OUT)
public class ResetFuzzIT {
    private static final int TEST_EXECUTION_TIME = 2000;

    private static final String SHORT_QUERY_1 = "CREATE (n:Node {name: 'foo', occupation: 'bar'})";
    private static final String SHORT_QUERY_2 = "MATCH (n:Node {name: 'foo'}) RETURN count(n)";
    private static final String SHORT_QUERY_3 = "RETURN 1";
    private static final String LONG_QUERY = "UNWIND range(0, 10000000) AS i CREATE (n:Node {idx: i}) DELETE n";

    private final int seed = new Random().nextInt();
    private final Random rand = new Random(seed);

    private final AssertableLogProvider internalLogProvider = new SpiedAssertableLogProvider(Connection.class);
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();

    @Inject
    private Neo4jWithSocket server;

    private java.net.SocketAddress address;

    private final BoltWire wire = new BoltDefaultWire();

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        server.setGraphDatabaseFactory(getTestGraphDatabaseFactory());
        server.setConfigure(getSettingsFunction());
        server.init(testInfo);
        address = server.lookupDefaultConnector().toSocketAddress();
    }

    @AfterEach
    public void tearDown() {
        userLogProvider.print(System.out);
        internalLogProvider.print(System.out);
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void shouldTerminateAutoCommitQuery() throws Exception {
        var sequences = new RequestSequenceCollection()
                .with(wire.run(SHORT_QUERY_1), wire.pull())
                .with(wire.run(SHORT_QUERY_2), wire.discard())
                .with(wire.run(SHORT_QUERY_3));

        execute(sequences);
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void shouldTerminateLongRunningAutoCommitQuery() throws Exception {
        // It takes a while for kernel to notice the tx get killed.
        var sequences = new RequestSequenceCollection().with(wire.run(LONG_QUERY), wire.discard());

        execute(sequences);
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void shouldTerminateQueryInExplicitTransaction() throws Exception {
        var sequences = new RequestSequenceCollection()
                .with(wire.begin(), wire.run(SHORT_QUERY_1), wire.pull(), wire.rollback())
                .with(wire.begin(), wire.run(SHORT_QUERY_2), wire.pull(), wire.commit())
                .with(wire.begin(), wire.run(SHORT_QUERY_3), wire.pull())
                .with(wire.begin(), wire.run(SHORT_QUERY_1))
                .with(wire.begin());

        execute(sequences);
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void shouldTerminateLongRunningQueryInExplicitTransaction() throws Exception {
        var sequences =
                new RequestSequenceCollection().with(wire.begin(), wire.run(LONG_QUERY), wire.pull(), wire.rollback());

        execute(sequences);
    }

    private void execute(RequestSequenceCollection sequences) throws Exception {
        var connection = connectAndAuthenticate();
        long deadline = System.currentTimeMillis() + TEST_EXECUTION_TIME;

        // when
        while (System.currentTimeMillis() < deadline) {
            var request = sequences.execute(connection, rand);

            connection.send(wire.reset());

            request.assertResponseOrRecord(connection);
            assertThat(connection).receivesSuccess();
        }
    }

    private TransportConnection connectAndAuthenticate() throws Exception {
        var connection = new SocketConnection(address)
                .connect()
                .sendDefaultProtocolVersion()
                .send(wire.hello());

        assertThat(connection).negotiatesDefaultVersion();

        assertThat(connection).receivesSuccess();

        return connection;
    }

    private TestDatabaseManagementServiceBuilder getTestGraphDatabaseFactory() {
        TestDatabaseManagementServiceBuilder factory = new TestDatabaseManagementServiceBuilder();
        factory.setInternalLogProvider(internalLogProvider);
        factory.setUserLogProvider(userLogProvider);
        return factory;
    }

    private static Consumer<Map<Setting<?>, Object>> getSettingsFunction() {
        return settings -> {
            settings.put(BoltConnector.encryption_level, OPTIONAL);
            settings.put(BoltConnector.listen_address, new SocketAddress("localhost", 0));
            settings.put(BoltConnectorInternalSettings.unsupported_thread_pool_queue_size, -1);
            settings.put(BoltConnector.thread_pool_min_size, 1);
            settings.put(BoltConnector.thread_pool_max_size, 1);
        };
    }
}
