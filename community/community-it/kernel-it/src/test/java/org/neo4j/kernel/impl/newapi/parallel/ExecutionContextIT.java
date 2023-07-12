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
package org.neo4j.kernel.impl.newapi.parallel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.io.ByteUnit.bytes;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.IOUtils.closeAllUnchecked;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionExecutionStatistic;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.time.Clocks;
import org.neo4j.util.concurrent.Futures;

@DbmsExtension
public class ExecutionContextIT {
    private static final int NUMBER_OF_WORKERS = 20;

    @Inject
    private GraphDatabaseAPI databaseAPI;

    private ExecutorService executors;

    @BeforeEach
    void setUp() {
        executors = Executors.newFixedThreadPool(NUMBER_OF_WORKERS);
    }

    @AfterEach
    void tearDown() {
        executors.shutdown();
    }

    @RepeatedTest(10)
    void contextMemoryTracking() throws ExecutionException {
        try (Transaction transaction = databaseAPI.beginTx()) {
            var ktx = (KernelTransactionImplementation) ((InternalTransaction) transaction).kernelTransaction();
            try (Statement statement = ktx.acquireStatement()) {
                var futures = new ArrayList<Future<?>>(NUMBER_OF_WORKERS);
                var contexts = new ArrayList<ExecutionContext>(NUMBER_OF_WORKERS);
                for (int i = 0; i < NUMBER_OF_WORKERS; i++) {
                    var executionContext = ktx.createExecutionContext();
                    futures.add(executors.submit(() -> {
                        for (int j = 0; j < 5; j++) {
                            executionContext.memoryTracker().allocateHeap(10);
                        }
                        executionContext.complete();
                    }));
                    contexts.add(executionContext);
                }
                Futures.getAll(futures);

                KernelTransactions kernelTransactions =
                        databaseAPI.getDependencyResolver().resolveDependency(KernelTransactions.class);

                var transactionHandle = kernelTransactions.activeTransactions().stream()
                        .filter(tx -> tx.isUnderlyingTransaction(ktx))
                        .findFirst()
                        .orElseThrow();
                assertEquals(
                        kibiBytes(128 * NUMBER_OF_WORKERS),
                        transactionHandle.transactionStatistic().getEstimatedUsedHeapMemory());
                assertEquals(0, transactionHandle.transactionStatistic().getNativeAllocatedBytes());

                closeAllUnchecked(contexts);

                assertEquals(
                        bytes(5 * 10 * NUMBER_OF_WORKERS),
                        transactionHandle.transactionStatistic().getEstimatedUsedHeapMemory());
                assertEquals(0, transactionHandle.transactionStatistic().getNativeAllocatedBytes());

                transaction.close();

                var statistic = new TransactionExecutionStatistic(ktx, Clocks.nanoClock(), 0);
                assertEquals(0, statistic.getEstimatedUsedHeapMemory());
                assertEquals(0, statistic.getNativeAllocatedBytes());
            }
        }
    }

    @RepeatedTest(10)
    void contextAccessNodeExist() throws ExecutionException {
        int numberOfNodes = 1024;
        long[] nodeIds = new long[numberOfNodes];
        try (var transaction = databaseAPI.beginTx()) {
            for (int i = 0; i < numberOfNodes; i++) {
                Node node = transaction.createNode();
                nodeIds[i] = node.getId();
            }
            transaction.commit();
        }

        try (Transaction transaction = databaseAPI.beginTx()) {
            var ktx = ((InternalTransaction) transaction).kernelTransaction();
            try (Statement statement = ktx.acquireStatement()) {
                var futures = new ArrayList<Future<?>>(NUMBER_OF_WORKERS);
                var contexts = new ArrayList<ExecutionContext>(NUMBER_OF_WORKERS);
                for (int i = 0; i < NUMBER_OF_WORKERS; i++) {
                    var executionContext = ktx.createExecutionContext();
                    futures.add(executors.submit(() -> {
                        for (long nodeId : nodeIds) {
                            assertTrue(executionContext.dataRead().nodeExists(nodeId));
                        }
                        executionContext.complete();
                    }));
                    contexts.add(executionContext);
                }
                Futures.getAll(futures);
                closeAllUnchecked(contexts);
            }
        }
    }

    @RepeatedTest(10)
    void contextAccessRelationshipExist() throws ExecutionException {
        int numberOfRelationships = 1024;
        long[] relIds = new long[numberOfRelationships];
        try (var transaction = databaseAPI.beginTx()) {
            for (int i = 0; i < numberOfRelationships; i++) {
                Node start = transaction.createNode();
                Node end = transaction.createNode();
                var relationship = start.createRelationshipTo(end, RelationshipType.withName("maker"));
                relIds[i] = relationship.getId();
            }
            transaction.commit();
        }

        try (Transaction transaction = databaseAPI.beginTx()) {
            var ktx = ((InternalTransaction) transaction).kernelTransaction();
            try (Statement statement = ktx.acquireStatement()) {
                var futures = new ArrayList<Future<?>>(NUMBER_OF_WORKERS);
                var contexts = new ArrayList<ExecutionContext>(NUMBER_OF_WORKERS);
                for (int i = 0; i < NUMBER_OF_WORKERS; i++) {
                    var executionContext = ktx.createExecutionContext();
                    futures.add(executors.submit(() -> {
                        for (long relId : relIds) {
                            assertTrue(executionContext.dataRead().relationshipExists(relId));
                        }
                        executionContext.complete();
                    }));
                    contexts.add(executionContext);
                }
                Futures.getAll(futures);
                closeAllUnchecked(contexts);
            }
        }
    }

    @RepeatedTest(10)
    void contextPeriodicReport() throws ExecutionException {
        int numberOfNodes = 32768;
        long[] nodeIds = new long[numberOfNodes];
        try (var transaction = databaseAPI.beginTx()) {
            for (int i = 0; i < numberOfNodes; i++) {
                Node node = transaction.createNode();
                nodeIds[i] = node.getId();
            }
            transaction.commit();
        }
        int nodeSize = databaseAPI.databaseLayout() instanceof RecordDatabaseLayout
                ? NodeRecordFormat.RECORD_SIZE
                : 128; // 128B per node in block
        int nodesPerPage = PageCache.PAGE_SIZE / nodeSize;
        int numPages = (int) Math.ceil((double) numberOfNodes / nodesPerPage);
        int numPins = numPages * NUMBER_OF_WORKERS;

        try (Transaction transaction = databaseAPI.beginTx()) {
            var ktx = ((InternalTransaction) transaction).kernelTransaction();
            try (Statement statement = ktx.acquireStatement()) {
                var futures = new ArrayList<Future<?>>(NUMBER_OF_WORKERS);
                var contexts = new ArrayList<ExecutionContext>(NUMBER_OF_WORKERS);
                for (int i = 0; i < NUMBER_OF_WORKERS; i++) {
                    var executionContext = ktx.createExecutionContext();
                    futures.add(executors.submit(() -> {
                        for (long nodeId : nodeIds) {
                            assertTrue(executionContext.dataRead().nodeExists(nodeId));
                            if (nodeId % 100 == 0) {
                                executionContext.report();
                            }
                        }
                        executionContext.complete();
                    }));
                    contexts.add(executionContext);
                }
                Futures.getAll(futures);
                closeAllUnchecked(contexts);

                var tracer = ktx.cursorContext().getCursorTracer();
                assertEquals(numPins, tracer.pins());
                assertEquals(numPins, tracer.unpins());
                assertEquals(numPins, tracer.hits());
            }
        }
    }

    @Test
    void closingExecutionContextDoNotLeakCursors() {
        for (int i = 0; i < 1024; i++) {
            try (Transaction transaction = databaseAPI.beginTx()) {
                var ktx = ((InternalTransaction) transaction).kernelTransaction();
                try (var statement = ktx.acquireStatement();
                        var executionContext = ktx.createExecutionContext()) {
                    executionContext.complete();
                }
            }
        }
    }

    @Test
    void testTransactionTerminationCheck() {
        try (Transaction transaction = databaseAPI.beginTx()) {
            var ktx = ((InternalTransaction) transaction).kernelTransaction();
            try (Statement statement = ktx.acquireStatement();
                    var executionContext = ktx.createExecutionContext()) {
                try {
                    var read = executionContext.dataRead();
                    ktx.markForTermination(Status.Transaction.Terminated);
                    assertThatThrownBy(() -> read.nodeExists(1))
                            .isInstanceOf(TransactionTerminatedException.class)
                            .hasMessageContaining("The transaction has been terminated.");
                } finally {
                    executionContext.complete();
                }
            }
        }
    }

    @Test
    void shouldDetectWhenExecutionContextOutlivesItsTransaction() {
        ExecutionContext executionContext;
        KernelTransaction originalKtx;
        try (Transaction transaction = databaseAPI.beginTx()) {
            originalKtx = ((InternalTransaction) transaction).kernelTransaction();
            try (Statement statement = originalKtx.acquireStatement()) {
                executionContext = originalKtx.createExecutionContext();
            }
        }

        List<Transaction> transactions = new ArrayList<>();

        try {
            // There might be more than one kernel transaction in the pool.
            while (true) {
                if (transactions.size() > 100) {
                    // Just to make sure we don't end up in an infinite loop if something changes
                    fail("Failed to get the original kernel transactions");
                }
                Transaction transaction = databaseAPI.beginTx();
                transactions.add(transaction);

                var ktx = ((InternalTransaction) transaction).kernelTransaction();

                if (originalKtx == ktx) {
                    assertThatThrownBy(() -> executionContext.dataRead().nodeExists(1))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("Execution context used after transaction close");
                    break;
                }
            }
        } finally {
            transactions.forEach(Transaction::close);
            executionContext.complete();
            executionContext.close();
        }
    }

    @Test
    void shouldFailToCrateExecutionContextForTransactionWithState() {
        try (Transaction transaction = databaseAPI.beginTx()) {
            transaction.createNode();
            var ktx = ((InternalTransaction) transaction).kernelTransaction();
            assertThatThrownBy(ktx::createExecutionContext)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "Execution context cannot be used for transactions with non-empty transaction state");
        }
    }

    @Test
    void executionContextShouldManageResources() throws Exception {
        try (Transaction transaction = databaseAPI.beginTx()) {
            var kts = ((InternalTransaction) transaction).kernelTransaction();
            try (Statement statement = kts.acquireStatement()) {
                var executionContext = kts.createExecutionContext();
                var resource1 = mock(AutoCloseable.class);
                var resource2 = mock(AutoCloseable.class);
                var resource3 = mock(AutoCloseable.class);

                executionContext.registerCloseableResource(resource1);
                executionContext.registerCloseableResource(resource2);
                executionContext.registerCloseableResource(resource3);
                executionContext.unregisterCloseableResource(resource2);

                executionContext.complete();
                executionContext.close();

                verify(resource1).close();
                verify(resource2, never()).close();
                verify(resource3).close();
            }
        }
    }

    @Test
    void shouldFailToCloseIfNotCompleted() {
        try (Transaction transaction = databaseAPI.beginTx()) {
            var kts = ((InternalTransaction) transaction).kernelTransaction();
            try (Statement statement = kts.acquireStatement()) {
                var executionContext = kts.createExecutionContext();
                assertThatThrownBy(executionContext::close)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("Execution context closed before it was marked as completed.");
            }
        }
    }

    @Test
    void testStateCheckWhenTransactionClosed() {
        try (Transaction transaction = databaseAPI.beginTx()) {
            var kts = ((InternalTransaction) transaction).kernelTransaction();
            try (Statement statement = kts.acquireStatement()) {
                var executionContext = kts.createExecutionContext();
                executionContext.performCheckBeforeOperation();
                assertThat(executionContext.isTransactionOpen()).isTrue();

                transaction.close();

                assertThat(executionContext.isTransactionOpen()).isFalse();
                assertThatThrownBy(executionContext::performCheckBeforeOperation)
                        .isInstanceOf(NotInTransactionException.class)
                        .hasMessage("This transaction has already been closed.");
            }
        }
    }

    @Test
    void testStateCheckWhenTransactionTerminated() {
        try (Transaction transaction = databaseAPI.beginTx()) {
            var kts = ((InternalTransaction) transaction).kernelTransaction();
            try (Statement statement = kts.acquireStatement()) {
                var executionContext = kts.createExecutionContext();
                executionContext.performCheckBeforeOperation();
                assertThat(executionContext.isTransactionOpen()).isTrue();

                transaction.terminate();

                assertThat(executionContext.isTransactionOpen()).isFalse();
                assertThatThrownBy(executionContext::performCheckBeforeOperation)
                        .isInstanceOf(TransactionTerminatedException.class)
                        .hasMessageContaining("The transaction has been terminated");
            }
        }
    }
}
