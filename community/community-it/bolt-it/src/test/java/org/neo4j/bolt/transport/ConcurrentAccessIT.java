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
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.begin;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.hello;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.pull;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.rollback;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.run;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

/**
 * Multiple concurrent users should be able to connect simultaneously. We test this with multiple users running
 * load that they roll back, asserting they don't see each others changes.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class ConcurrentAccessIT extends AbstractBoltTransportsTest {
    @Inject
    private Neo4jWithSocket server;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        server.setConfigure(getSettingsFunction());
        server.init(testInfo);

        this.address = server.lookupDefaultConnector();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldRunSimpleStatement(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // Given
        int numWorkers = 5;
        int numRequests = 1_000;

        var workers = createWorkers(numWorkers, numRequests);
        var exec = Executors.newFixedThreadPool(numWorkers);

        try {
            // When & then
            for (var f : exec.invokeAll(workers)) {
                f.get(60, TimeUnit.SECONDS);
            }
        } finally {
            exec.shutdownNow();
            exec.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private List<Callable<Void>> createWorkers(int numWorkers, int numRequests) throws Exception {
        List<Callable<Void>> workers = new LinkedList<>();
        for (int i = 0; i < numWorkers; i++) {
            workers.add(newWorker(numRequests));
        }
        return workers;
    }

    private Callable<Void> newWorker(final int iterationsToRun) {
        return new Callable<>() {

            @Override
            public Void call() throws Exception {
                // Connect
                var connection = newConnection();

                connection.connect().sendDefaultProtocolVersion().send(hello());

                assertThat(connection).negotiatesDefaultVersion().receivesSuccess();

                for (int i = 0; i < iterationsToRun; i++) {
                    createAndRollback(connection);
                }

                return null;
            }

            private void createAndRollback(TransportConnection connection) throws Exception {
                connection.send(begin()).send(run("CREATE (n)")).send(pull()).send(rollback());

                assertThat(connection)
                        .receivesSuccess()
                        .receivesSuccess(meta -> assertThat(meta)
                                .containsKeys("t_first", "qid")
                                .hasEntrySatisfying("fields", fields -> assertThat(fields)
                                        .asInstanceOf(list(String.class))
                                        .isEmpty()))
                        .receivesSuccess(meta -> assertThat(meta).containsKeys("t_last", "db"))
                        .receivesSuccess();

                connection.send(run("MATCH (n) RETURN n")).send(pull());

                assertThat(connection)
                        .receivesSuccess(meta -> assertThat(meta)
                                .containsKeys("t_first")
                                .hasEntrySatisfying("fields", fields -> assertThat(fields)
                                        .asInstanceOf(list(String.class))
                                        .hasSize(1)
                                        .containsExactly("n")))
                        .receivesSuccess(meta -> assertThat(meta).containsKeys("t_last", "db"));
            }
        };
    }
}
