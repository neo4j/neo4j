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
package org.neo4j.kernel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_monitor_check_interval;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.TerminationMark;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.assertion.Assert;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.time.FakeClock;

@DbmsExtension(configurationCallback = "configure")
@ExtendWith(OtherThreadExtension.class)
public class StaleTransactionIT {
    private AssertableLogProvider logProvider = new AssertableLogProvider();
    private FakeClock clock = new FakeClock();

    @Inject
    private GraphDatabaseFacade db;

    @Inject
    private KernelTransactions transactions;

    @Inject
    private Database database;

    @Inject
    private OtherThread otherThread;

    @ExtensionCallback
    protected void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(transaction_monitor_check_interval, Duration.ofMillis(100))
                .setClock(clock)
                .setInternalLogProvider(logProvider);
    }

    @Test
    void testStaleTransactionLogged() throws InterruptedException, ExecutionException {
        CountDownLatch testStopLatch = new CountDownLatch(1);
        CountDownLatch staleTransactionStartLatch = new CountDownLatch(1);
        AtomicReference<KernelTransaction> kernelTxRef = new AtomicReference<>();

        // Another thread runs a transaction that does no work for a long
        // time (i.e. the duration of the test).
        var future = otherThread.execute(() -> {
            try (InternalTransaction tx =
                    db.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
                kernelTxRef.set(tx.kernelTransaction());
                staleTransactionStartLatch.countDown();
                testStopLatch.await();
            }
            return null;
        });

        try {
            assertThat(staleTransactionStartLatch.await(1, TimeUnit.MINUTES))
                    .as("other thread should have started transaction")
                    .isTrue();
            assertThat(kernelTxRef).doesNotHaveValue(null);
            var kernelTx = kernelTxRef.get();

            // We mark that transaction for termination...
            kernelTx.markForTermination(Status.Transaction.TransactionMarkedAsFailed);

            // ...and expect it to be marked as stale eventually.
            clock.forward(10, TimeUnit.MINUTES);
            Assert.assertEventually(
                    () -> kernelTx.getTerminationMark()
                            .map(TerminationMark::isMarkedAsStale)
                            .orElse(false),
                    stale -> stale,
                    1,
                    TimeUnit.MINUTES);

            // Once the mark has been set to stale there should also be a log message.
            LogAssertions.assertThat(logProvider)
                    .containsMessagesOnce("has been marked for termination for", "may have been leaked");
        } finally {
            // Clean up other thread: wait for it to complete and propagate any exceptions (suppressing those from
            // main thread)
            testStopLatch.countDown();
            future.get();
        }
    }
}
