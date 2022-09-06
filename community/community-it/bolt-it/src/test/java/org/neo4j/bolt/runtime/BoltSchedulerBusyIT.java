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
package org.neo4j.bolt.runtime;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
class BoltSchedulerBusyIT extends AbstractBoltTransportsTest {
    private final AssertableLogProvider internalLogProvider = new AssertableLogProvider();
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();

    @Inject
    private Neo4jWithSocket server;

    private TransportConnection connection1;
    private TransportConnection connection2;
    private TransportConnection connection3;
    private TransportConnection connection4;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        server.setGraphDatabaseFactory(getTestGraphDatabaseFactory());
        server.setConfigure(getSettingsFunction());
        server.init(testInfo);
        address = server.lookupDefaultConnector();
    }

    private TestDatabaseManagementServiceBuilder getTestGraphDatabaseFactory() {
        TestDatabaseManagementServiceBuilder factory = new TestDatabaseManagementServiceBuilder();
        factory.setInternalLogProvider(internalLogProvider);
        factory.setUserLogProvider(userLogProvider);
        return factory;
    }

    @Override
    protected Consumer<Map<Setting<?>, Object>> getSettingsFunction() {
        return settings -> {
            super.getSettingsFunction().accept(settings);
            settings.put(BoltConnector.listen_address, new SocketAddress("localhost", 0));
            settings.put(BoltConnector.thread_pool_min_size, 0);
            settings.put(BoltConnector.thread_pool_max_size, 2);
        };
    }

    @AfterEach
    public void cleanup() {
        close(connection1);
        close(connection2);
        close(connection3);
        close(connection4);
    }

    @ParameterizedTest(name = "{displayName} {1}")
    @MethodSource("argumentsProvider")
    public void shouldReportFailureWhenAllThreadsInThreadPoolAreBusy(TransportConnection.Factory connectionFactory)
            throws Throwable {
        initParameters(connectionFactory);

        // it's enough to get the bolt state machine into streaming mode to have
        // the thread stickied to the connection, causing all the available threads
        // to be busy (logically)
        connection1 = enterStreaming();
        connection2 = enterStreaming();

        try {
            connection3 = connectAndPerformBoltHandshake(newConnection());

            connection3.sendDefaultProtocolVersion().send(wire.hello());

            assertThat(connection3)
                    .receivesFailureFuzzy(
                            Status.Request.NoThreadsAvailable,
                            "There are no available threads to serve this request at the moment");

            assertThat(userLogProvider)
                    .forLevel(ERROR)
                    .containsMessages(
                            "since there are no available threads to serve it at the moment. You can retry at a later time");
            assertThat(internalLogProvider)
                    .forLevel(ERROR)
                    .containsMessages(
                            "since there are no available threads to serve it at the moment. You can retry at a later time");
        } finally {
            exitStreaming(connection1);
            exitStreaming(connection2);
        }
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldStopConnectionsWhenRelatedJobIsRejectedOnShutdown(TransportConnection.Factory connectionFactory)
            throws Throwable {
        initParameters(connectionFactory);

        // Connect and get two connections into idle state
        connection1 = enterStreaming();
        exitStreaming(connection1);
        connection2 = enterStreaming();
        exitStreaming(connection2);

        // Connect and get other set of connections to keep threads busy
        connection3 = enterStreaming();
        connection4 = enterStreaming();

        // Clear any log output till now
        internalLogProvider.clear();

        // Shutdown the server
        server.shutdownDatabase();

        // Expect no scheduling error logs
        assertThat(userLogProvider)
                .doesNotContainMessage(
                        "since there are no available threads to serve it at the moment. You can retry at a later time");
        assertThat(internalLogProvider)
                .doesNotContainMessage(
                        "since there are no available threads to serve it at the moment. You can retry at a later time");
    }

    private TransportConnection enterStreaming() throws Throwable {
        TransportConnection connection = null;
        Throwable error = null;

        // retry couple times because worker threads might seem busy
        for (int i = 1; i <= 7; i++) {
            try {
                connection = newConnection();
                enterStreaming(connection, i);
                return connection;
            } catch (Throwable t) {
                // failed to enter the streaming state, record the error and retry
                if (error == null) {
                    error = t;
                } else {
                    error.addSuppressed(t);
                }

                close(connection);
                SECONDS.sleep(i);
            }
        }

        throw error;
    }

    private void enterStreaming(TransportConnection connection, int sleepSeconds) throws Exception {
        connectAndPerformBoltHandshake(connection);

        connection.send(wire.hello());

        assertThat(connection).receivesSuccess();

        SECONDS.sleep(sleepSeconds); // sleep a bit to allow the worker thread to return to the pool

        connection.send(wire.run("UNWIND RANGE (1, 100) AS x RETURN x"));

        assertThat(connection).receivesSuccess();
    }

    private TransportConnection connectAndPerformBoltHandshake(TransportConnection connection) throws Exception {
        connection.connect().sendDefaultProtocolVersion();

        assertThat(connection).negotiatesDefaultVersion();

        return connection;
    }

    private void exitStreaming(TransportConnection connection) throws Exception {
        connection.send(wire.discard());

        assertThat(connection).receivesSuccess();
    }

    private static void close(TransportConnection connection) {
        if (connection != null) {
            try {
                connection.disconnect();
            } catch (IOException ignore) {
            }
        }
    }
}
