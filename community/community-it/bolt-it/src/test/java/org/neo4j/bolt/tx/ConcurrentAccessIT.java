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
package org.neo4j.bolt.tx;

import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.api.Assertions;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.provider.ConnectionProvider;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

/**
 * Ensures that Bolt serves concurrent connections.
 * <p />
 * To evaluate this, this test will let multiple workers perform various workloads against the database at the same
 * time. All changes will be rolled back (thus preventing workers from seeing each other's state).
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class ConcurrentAccessIT {

    private static final int NUM_WORKERS = 5;
    private static final int NUM_REQUESTS = 1_000;

    private void runWorkload(
            ConnectionProvider connectionProvider,
            int nWorkers,
            int nTimes,
            ThrowingConsumer<TransportConnection, IOException> workload)
            throws InterruptedException, BrokenBarrierException, TimeoutException {
        var pool = Executors.newFixedThreadPool(nWorkers);

        try {
            var barrier = new CyclicBarrier(nWorkers + 1);
            var errorCounter = new AtomicInteger();

            for (int i = 0; i < nWorkers; ++i) {
                pool.submit(() -> {
                    try {
                        // acquire an authenticated connection
                        var connection = connectionProvider.create();

                        // wait for all parties to reach the barrier point in order to synchronize startup
                        barrier.await();

                        // execute the actual workload n times
                        for (var j = 0; j < nTimes; ++j) {
                            workload.accept(connection);
                        }

                        // wait till all parties manage to execute the entire workload
                        barrier.await();
                    } catch (Throwable ex) {
                        ex.printStackTrace();

                        errorCounter.incrementAndGet();
                    }
                });
            }

            // wait until all workers managed to acquire a connection and start processing
            barrier.await(1, TimeUnit.MINUTES);

            // wait until all workers complete their assignment
            barrier.await(5, TimeUnit.MINUTES);

            // ensure that no errors were reported, otherwise forcefully fail the test (errors will be reported to
            // stderr)
            Assertions.assertThat(errorCounter).hasValue(0);
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    @ProtocolTest
    void shouldRunSimpleStatement(@Authenticated ConnectionProvider connectionProvider, BoltWire wire)
            throws Exception {
        this.runWorkload(connectionProvider, NUM_WORKERS, NUM_REQUESTS, connection -> {
            connection
                    .send(wire.begin())
                    .send(wire.run("CREATE (n)"))
                    .send(wire.pull())
                    .send(wire.rollback());

            assertThat(connection)
                    .receivesSuccess()
                    .receivesSuccess(meta -> Assertions.assertThat(meta)
                            .containsKeys("t_first", "qid")
                            .hasEntrySatisfying("fields", fields -> Assertions.assertThat(fields)
                                    .asInstanceOf(list(String.class))
                                    .isEmpty()))
                    .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("t_last", "db"))
                    .receivesSuccess();

            connection.send(wire.run("MATCH (n) RETURN n")).send(wire.pull());

            assertThat(connection)
                    .receivesSuccess(meta -> Assertions.assertThat(meta)
                            .containsKeys("t_first")
                            .hasEntrySatisfying("fields", fields -> Assertions.assertThat(fields)
                                    .asInstanceOf(list(String.class))
                                    .hasSize(1)
                                    .containsExactly("n")))
                    .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("t_last", "db"));
        });
    }
}
