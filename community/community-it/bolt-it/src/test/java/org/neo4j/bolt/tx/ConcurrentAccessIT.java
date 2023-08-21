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
package org.neo4j.bolt.tx;

import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.util.concurrent.Futures.getAllResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import org.neo4j.internal.batchimport.HighestId;
import org.neo4j.test.DoubleLatch;
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
    private static final int NUM_WORKERS = 4;
    private static final int NUM_REQUESTS = 100;

    private void runWorkload(
            ConnectionProvider connectionProvider,
            int nWorkers,
            int nTimes,
            ThrowingConsumer<TransportConnection, IOException> workload)
            throws InterruptedException, ExecutionException {
        var barrier = new DoubleLatch(nWorkers);
        var tasks = new ArrayList<Callable<Void>>();
        var numActiveWorkers = new AtomicInteger();
        var highestNumConcurrentWorkers = new HighestId();
        for (var i = 0; i < nWorkers; ++i) {
            tasks.add(() -> {
                // acquire an authenticated connection
                var connection = connectionProvider.create();

                // wait for all parties to reach the barrier point in order to synchronize startup
                barrier.startAndWaitForAllToStart();
                highestNumConcurrentWorkers.offer(numActiveWorkers.incrementAndGet());

                // execute the actual workload n times
                for (var j = 0; j < nTimes; ++j) {
                    workload.accept(connection);
                }

                // wait till all parties manage to execute the entire workload
                barrier.finishAndWaitForAllToFinish();
                return null;
            });
        }

        var pool = Executors.newFixedThreadPool(nWorkers);
        try {
            var futures = pool.invokeAll(tasks);

            // when
            getAllResults(futures);

            // then no exception is thrown, and
            Assertions.assertThat(highestNumConcurrentWorkers.get()).isEqualTo(nWorkers);
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
