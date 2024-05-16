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
package org.neo4j.bolt;

import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doAnswer;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.values.storable.Values.stringValue;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.connection.transport.ExcludeTransport;
import org.neo4j.bolt.test.annotation.setup.FactoryFunction;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.test.util.ServerUtil;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.SpiedAssertableLogProvider;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

/**
 * Ensures that Bolt correctly terminates connections when the server is shut down gracefully.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExtendWith(OtherThreadExtension.class)
@ExcludeTransport(TransportType.UNIX) // UNIX behavior may differ on some operating systems
public class ShutdownSequenceIT {
    private static final Duration THREAD_POOL_SHUTDOWN_WAIT_TIME = Duration.ofMinutes(10);

    private final AssertableLogProvider internalLogProvider = new SpiedAssertableLogProvider(BoltServer.class);
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();

    @Inject
    private Neo4jWithSocket server;

    private CountDownLatch txStarted;
    private CountDownLatch boltWorkerThreadPoolShuttingDown;

    @FactoryFunction
    void customizeServer(TestDatabaseManagementServiceBuilder factory) {
        factory.setInternalLogProvider(this.internalLogProvider);
        factory.setUserLogProvider(this.userLogProvider);
    }

    @SettingsFunction
    static void customizeSettings(Map<Setting<?>, Object> settings) {
        settings.put(BoltConnector.thread_pool_min_size, 0);
        settings.put(BoltConnector.thread_pool_max_size, 2);
        settings.put(BoltConnectorInternalSettings.thread_pool_shutdown_wait_time, THREAD_POOL_SHUTDOWN_WAIT_TIME);
    }

    @BeforeEach
    void prepare() throws KernelException {
        this.txStarted = new CountDownLatch(1);
        this.boltWorkerThreadPoolShuttingDown = new CountDownLatch(1);

        ServerUtil.registerComponent(
                this.server, Pair.class, context -> Pair.of(this.txStarted, this.boltWorkerThreadPoolShuttingDown));
        ServerUtil.installProcedure(this.server, TestProcedures.class);
    }

    @AfterEach
    void cleanup() {
        this.userLogProvider.clear();
        this.internalLogProvider.clear();
    }

    @TransportTest
    void shouldReturnFailureForTransactionAwareConnections(BoltWire wire, @Authenticated TransportConnection connection)
            throws IOException, InterruptedException {
        connection.send(wire.run("CALL test.stream.nodes()")).send(wire.pull());

        // Wait for a transaction to start on the server side
        assertTrue(txStarted.await(1, MINUTES));

        // Register a callback when the bolt worker thread pool is shut down.
        var boltLog = internalLogProvider.getLog(BoltServer.class);
        doAnswer(invocation -> {
                    invocation.callRealMethod();
                    boltWorkerThreadPoolShuttingDown.countDown();
                    return null;
                })
                .when(boltLog)
                .info("Shutting down Bolt server");

        // Shutdown the server
        server.getManagementService().shutdown();

        // Expect the connection to have the following interactions
        assertThat(connection)
                .receivesSuccess()
                .receivesFailure(
                        meta -> assertThat(meta)
                                // todo this should not be a transient error such as Status.Transaction.Terminated
                                .containsEntry(
                                        "code",
                                        Status.General.DatabaseUnavailable.code()
                                                .serialize())
                                .containsEntry(
                                        "message",
                                        "The transaction has been terminated. Retry your operation in a new transaction, "
                                                + "and you should see a successful result. The database is not currently available to serve your request, "
                                                + "refer to the database logs for more details. Retrying your request at a later time may succeed. "))
                .isEventuallyTerminated();

        assertThat(internalLogProvider)
                .forClass(BoltServer.class)
                .forLevel(INFO)
                .containsMessages("Bolt server has been shut down");
    }

    @TransportTest
    void shutdownShouldCloseIdleConnections(@Authenticated TransportConnection connection) throws IOException {
        // Shutdown the server
        server.getManagementService().shutdown();

        // Expect the connection to be silently closed.
        assertThat(connection).isEventuallyTerminated();

        assertThat(internalLogProvider)
                .forClass(BoltServer.class)
                .forLevel(INFO)
                .containsMessages("Bolt server has been shut down");
    }

    @TransportTest
    void shutdownShouldWaitForNonTransactionAwareConnections(
            BoltWire wire, @Authenticated TransportConnection connection) throws IOException, InterruptedException {
        connection.send(wire.run("CALL test.stream.strings()")).send(wire.pull());

        // Wait for a transaction to start on the server side
        assertTrue(txStarted.await(1, MINUTES));

        // Register a callback when the bolt worker thread pool is shut down.
        var boltLog = internalLogProvider.getLog(BoltServer.class);
        doAnswer(invocation -> {
                    invocation.callRealMethod();
                    boltWorkerThreadPoolShuttingDown.countDown();
                    return null;
                })
                .when(boltLog)
                .info("Shutting down Bolt server");

        // Initiate the shutdown
        server.getManagementService().shutdown();

        // Expect the connection to have the following interactions
        assertThat(connection)
                .receivesSuccess()
                .receivesRecord(record -> assertThat(record).hasSize(1).contains(stringValue("0")))
                .receivesFailure(
                        meta -> assertThat(meta)
                                // todo this should not be a transient error such as Status.Transaction.Terminated
                                .containsEntry(
                                        "code",
                                        Status.General.DatabaseUnavailable.code()
                                                .serialize())
                                .containsEntry(
                                        "message",
                                        "The transaction has been terminated. Retry your operation in a new transaction, "
                                                + "and you should see a successful result. The database is not currently available to serve your request, "
                                                + "refer to the database logs for more details. Retrying your request at a later time may succeed. "))
                .isEventuallyTerminated();

        assertThat(internalLogProvider)
                .forClass(BoltServer.class)
                .forLevel(INFO)
                .containsMessages("Bolt server has been shut down");
    }

    public static class TestProcedures {
        @Context
        public GraphDatabaseService db;

        @Context
        public Pair<CountDownLatch, CountDownLatch> pair;

        @Context
        public Transaction tx;

        @Procedure(name = "test.stream.strings", mode = READ)
        public Stream<Output> streamStrings() {
            pair.first().countDown();
            try {
                assertTrue(pair.other().await(1, MINUTES));
            } catch (InterruptedException e) {
                fail("Interrupted while waiting for bolt worker threads shut down.");
            }
            // I shall be able to stream this value back.
            // But this procedure tx shall not be able to commit/rollback as dbms is already shutting down.
            return Stream.of(new Output(valueOf(0)));
        }

        @Procedure(name = "test.stream.nodes", mode = READ)
        public Stream<Output> streamNodes() {
            pair.first().countDown();
            try {
                assertTrue(pair.other().await(1, MINUTES));
            } catch (InterruptedException e) {
                fail("Interrupted while waiting for bolt worker threads shut down.");
            }

            // I shall fail to access node id
            tx.getNodeById(0);
            return Stream.of(new Output(valueOf(0)));
        }

        public static class Output {
            public String out;

            Output(String value) {
                this.out = value;
            }
        }
    }
}
