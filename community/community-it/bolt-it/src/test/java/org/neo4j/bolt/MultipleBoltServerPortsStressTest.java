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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.bolt.testing.assertions.ListValueAssertions.assertThat;
import static org.neo4j.values.storable.Values.longValue;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.messages.BoltV44Wire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
@Neo4jWithSocketExtension
class MultipleBoltServerPortsStressTest {
    private static final int DURATION_IN_MINUTES = 1;
    private static final int NUMBER_OF_THREADS = 10;

    @Inject
    public Neo4jWithSocket server;

    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        server.setGraphDatabaseFactory(new SharedAuthManagerDbmsBuilder());
        server.setConfigure(settings -> {
            settings.put(BoltConnector.enabled, true);
            settings.put(BoltConnector.listen_address, new SocketAddress(0));

            settings.put(GraphDatabaseSettings.routing_enabled, true);
            settings.put(GraphDatabaseSettings.routing_listen_address, new SocketAddress(0));
        });
        server.init(testInfo);
    }

    @Test
    void splitTrafficBetweenPorts() throws Exception {
        HostnamePort externalAddress = server.lookupConnector(ConnectorType.BOLT);
        HostnamePort internalAddress = server.lookupConnector(ConnectorType.INTRA_BOLT);

        executeStressTest(Executors.newFixedThreadPool(NUMBER_OF_THREADS), externalAddress, internalAddress);
    }

    private static void executeStressTest(ExecutorService executorPool, HostnamePort external, HostnamePort internal)
            throws Exception {
        long finishTimeMillis =
                System.currentTimeMillis() + MINUTES.toMillis(MultipleBoltServerPortsStressTest.DURATION_IN_MINUTES);
        AtomicBoolean failureFlag = new AtomicBoolean(false);

        for (int i = 0; i < MultipleBoltServerPortsStressTest.NUMBER_OF_THREADS; i++) {
            SocketConnection connection;

            // split connections evenly between internal and external
            if (i % 2 == 0) {
                connection = new SocketConnection(internal);
            } else {
                connection = new SocketConnection(external);
            }

            connection.connect().sendDefaultProtocolVersion().send(BoltV44Wire.hello());

            assertThat(connection).negotiatesDefaultVersion();
            assertThat(connection).receivesSuccess();

            executorPool.submit(workload(failureFlag, connection, finishTimeMillis));
        }

        executorPool.shutdown();
        executorPool.awaitTermination(DURATION_IN_MINUTES, MINUTES);
        assertThat(failureFlag).isFalse();
    }

    private static Condition<AnyValue> longValueCondition() {
        return new Condition<>(value -> value.equals(longValue(1)), "equals");
    }

    private static Runnable workload(AtomicBoolean failureFlag, SocketConnection connection, long finishTimeMillis) {
        return () -> {
            while (!failureFlag.get() && System.currentTimeMillis() < finishTimeMillis) {
                try {
                    connection.send(BoltV44Wire.run("RETURN 1")).send(BoltV44Wire.pull());

                    assertThat(connection)
                            .receivesSuccess()
                            .receivesRecord(
                                    record -> assertThat(record).hasSize(1).contains(Values.longValue(1)))
                            .receivesSuccess();
                } catch (AssertionError | IOException e) {
                    e.printStackTrace();
                    failureFlag.set(true);
                }
            }
        };
    }

    private static class SharedAuthManagerDbmsBuilder extends TestDatabaseManagementServiceBuilder {
        @Override
        protected Function<GlobalModule, AbstractEditionModule> getEditionFactory(Config config) {
            return globalModule -> new CommunityEditionModule(globalModule) {
                @Override
                public AuthManager getBoltInClusterAuthManager() {
                    return getBoltAuthManager(globalModule.getGlobalDependencies());
                }
            };
        }
    }
}
