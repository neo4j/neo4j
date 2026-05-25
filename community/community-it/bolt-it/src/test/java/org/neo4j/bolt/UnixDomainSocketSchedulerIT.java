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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.connection.transport.ExcludeTransport;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.connection.transport.UseTransport;
import org.neo4j.bolt.test.annotation.setup.FactoryFunction;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.test.provider.ConnectionProvider;
import org.neo4j.bolt.test.util.ServerUtil;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@DisabledOnOs(OS.WINDOWS)
public class UnixDomainSocketSchedulerIT {

    private final AssertableLogProvider internalLogProvider = new AssertableLogProvider();
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();

    @Inject
    private Neo4jWithSocket server;

    private BoltServer boltServer() {
        var gdb = (GraphDatabaseAPI) server.graphDatabaseService();
        return gdb.getDependencyResolver().resolveDependency(BoltServer.class);
    }

    @FactoryFunction
    void customizeDatabase(TestDatabaseManagementServiceBuilder factory) {
        factory.setInternalLogProvider(this.internalLogProvider);
        factory.setUserLogProvider(this.userLogProvider);
    }

    @SettingsFunction
    static void customizeSettings(Map<Setting<?>, Object> settings) {
        settings.put(BoltConnector.thread_pool_min_size, 0);
        settings.put(BoltConnector.thread_pool_max_size, 1);
        settings.put(BoltConnectorInternalSettings.enable_unix_socket_user_database_access, true);

        settings.put(BoltConnector.unix_socket_use_dedicated_thread_pool, true);
        settings.put(BoltConnector.unix_socket_dedicated_thread_pool_min_size, 0);
        settings.put(BoltConnector.unix_socket_dedicated_thread_pool_max_size, 2);
    }

    /**
     * Evaluates whether UNIX domain sockets cannot be blocked by standard user connections when the
     * dedicated pool has been enabled.
     */
    @TransportTest
    @ExcludeTransport(TransportType.UNIX)
    void shouldProvideDedicatedPoolForUnixDomainSocket(
            BoltWire wire,
            @Authenticated BoltTestConnection standardConnection,
            @Authenticated @UseTransport(TransportType.UNIX) ConnectionProvider unixConnectionProvider)
            throws Exception {
        // saturate the primary thread pool with a simple streaming job and ensure that it is actually
        // busy
        enterStreaming(wire, standardConnection);
        ServerUtil.awaitPrimaryThreadPoolSaturation(boltServer(), 1);

        // create a new UNIX domain socket connection and ensure that it is still responsive regardless
        // of the primary pool being fully in use
        try (var unixConnection = unixConnectionProvider.create()) {
            unixConnection.send(wire.run("RETURN 1"));

            BoltConnectionAssertions.assertThat(unixConnection).receivesSuccess();
        }

        exitStreaming(wire, standardConnection);
        ServerUtil.awaitPrimaryThreadPoolSaturation(boltServer(), 0);
    }

    /**
     * Evaluates whether UNIX domain sockets are placed on the correct pool.
     */
    @TransportTest
    @IncludeTransport(TransportType.UNIX)
    void shouldNotSubmitToPrimaryPool(
            BoltWire wire, @Authenticated BoltTestConnection connection1, @Authenticated BoltTestConnection connection2)
            throws IOException, InterruptedException {
        enterStreaming(wire, connection1);
        enterStreaming(wire, connection2);

        var executor = (ThreadPoolExecutor) boltServer().getPrimaryExecutorService();
        ServerUtil.awaitDomainSocketThreadPoolSaturation(boltServer(), 2);

        var i = 0;
        do {
            Thread.sleep(100);

            assertThat(executor.getActiveCount()).isZero();
        } while (i++ < 10);
    }

    /**
     * Evaluates whether threads available to the UNIX domain sockets connector are limited based on
     * their respective configuration properties.
     */
    @TransportTest
    @IncludeTransport(TransportType.UNIX)
    void shouldAdhereToConfiguredThreadLimits(
            BoltWire wire,
            @Authenticated BoltTestConnection connection1,
            @Authenticated BoltTestConnection connection2,
            @VersionSelected ConnectionProvider connectionProvider)
            throws IOException {
        enterStreaming(wire, connection1);
        enterStreaming(wire, connection2);

        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInSameThread()
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    try (var connection3 = connectionProvider.create()) {
                        connection3.send(wire.hello());

                        BoltConnectionAssertions.assertThat(connection3)
                                .receivesFailureFuzzyV40(
                                        Status.Request.NoThreadsAvailable,
                                        "There are no available threads to serve this request at the moment");

                        BoltConnectionAssertions.assertThat(connection3).isEventuallyTerminated();
                    }
                });

        LogAssertions.assertThat(userLogProvider)
                .forLevel(DEBUG)
                .containsMessages("since there are no available threads to serve it at the moment.");
        LogAssertions.assertThat(internalLogProvider)
                .forLevel(DEBUG)
                .containsMessages("since there are no available threads to serve it at the moment.");
    }

    private static void enterStreaming(BoltWire wire, BoltTestConnection connection) throws IOException {
        connection.send(wire.run("UNWIND RANGE (1, 100) AS x RETURN x"));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    private static void exitStreaming(BoltWire wire, BoltTestConnection connection) throws IOException {
        connection.send(wire.discard());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }
}
