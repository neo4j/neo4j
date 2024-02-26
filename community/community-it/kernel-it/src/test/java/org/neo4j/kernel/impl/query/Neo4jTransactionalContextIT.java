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
package org.neo4j.kernel.impl.query;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.config.MEMORY_TRACKING;
import org.neo4j.cypher.internal.runtime.memory.QueryMemoryTracker;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryRegistry;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QueryObfuscator;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.FacadeKernelTransactionFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.KernelTransactionFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.builtin.TransactionId;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

@ImpermanentDbmsExtension
class Neo4jTransactionalContextIT {

    @Inject
    private DatabaseManagementService dbms;

    @Inject
    private GraphDatabaseAPI databaseAPI;

    @Inject
    private GraphDatabaseQueryService graph;

    private KernelTransactionFactory transactionFactory;

    // Helpers

    private long getPageCacheHits(TransactionalContext ctx) {
        return ctx.transaction().kernelTransaction().executionStatistics().pageHits();
    }

    private long getPageCacheFaults(TransactionalContext ctx) {
        return ctx.transaction().kernelTransaction().executionStatistics().pageFaults();
    }

    /**
     * Generate some page cache hits/faults
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void generatePageCacheHits(TransactionalContext ctx) {
        long previousCacheHits = getPageCacheHits(ctx);
        Iterables.count(ctx.transaction().getAllNodes());
        long laterCacheHits = getPageCacheHits(ctx);
        assertThat(laterCacheHits)
                .as("Assuming generatePageCacheHits to generate some page cache hits")
                .isGreaterThan(previousCacheHits);
    }

    /**
     * Generate some page cache hits/faults on commit; returns expected hits
     */
    private void generatePageCacheHitsOnCommit(TransactionalContext ctx) {
        ctx.transaction().createNode();
    }

    private long createNodeAndRecordNumberOfPageHits() {
        var tx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(tx);
        var executingQuery = ctx.executingQuery();
        ctx.transaction().createNode();
        ctx.commit();
        var pageHits = executingQuery.snapshot().pageHits();
        // Surely we expect at least some page hit here
        assertThat(pageHits).isGreaterThan(0);
        return pageHits;
    }

    private void getLocks(TransactionalContext ctx, String label) {
        try (ResourceIterator<Node> nodes = ctx.transaction().findNodes(Label.label(label))) {
            nodes.stream().forEach(Node::delete);
        }
    }

    private long getActiveLockCount(TransactionalContext ctx) {
        return ((KernelStatement) ctx.statement()).locks().activeLockCount();
    }

    private boolean isMarkedForTermination(TransactionalContext ctx) {
        return ctx.transaction().terminationReason().isPresent();
    }

    private TransactionalContext createTransactionContext(InternalTransaction transaction) {
        return Neo4jTransactionalContextFactory.create(() -> graph, transactionFactory)
                .newContext(transaction, "no query", EMPTY_MAP, QueryExecutionConfiguration.DEFAULT_CONFIG);
    }

    @BeforeEach
    void setup() {
        transactionFactory =
                new FacadeKernelTransactionFactory(Config.newBuilder().build(), (GraphDatabaseFacade) databaseAPI);
    }

    public static class Procedures {
        @Context
        public Transaction transaction;

        @Procedure(name = "test.failingProc", mode = Mode.WRITE)
        public void stupidProcedure() {
            transaction.execute("CREATE (c {prop: 1 / 0})");
        }
    }

    @Test
    void nestedQueriesWithExceptionsShouldCleanUpProperly() throws KernelException {
        // Given
        databaseAPI
                .getDependencyResolver()
                .resolveDependency(GlobalProcedures.class)
                .registerProcedure(Neo4jTransactionalContextIT.Procedures.class);

        var tx = graph.beginTransaction(EXPLICIT, LoginContext.AUTH_DISABLED);

        // When
        // Run a query which calls a procedure which runs an inner query which fails.
        var exception = assertThrows(
                QueryExecutionException.class, () -> tx.execute("CREATE (c) WITH c CALL test.failingProc()"));

        // Then
        assertNoSuppressedExceptions(exception);
        // all exceptions should reference what actually was the problem
        assertAllCauses(exception, e -> e.getMessage().contains("/ by zero"));
    }

    private void assertAllCauses(Throwable t, Predicate<Throwable> predicate) {
        assertTrue(predicate.test(t), "Predicate failed on " + t);
        if (t.getCause() != null) {
            assertAllCauses(t.getCause(), predicate);
        }
    }

    private void assertNoSuppressedExceptions(Throwable t) {
        if (t.getSuppressed().length > 0) {
            fail("Expected no suppressed exceptions. Got: " + Arrays.toString(t.getSuppressed()));
        }
        if (t.getCause() != null) {
            assertNoSuppressedExceptions(t.getCause());
        }
    }

    @Test
    void contextWithNewTransactionExecutingQueryShouldUseOuterTransactionIdAndQueryText() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var queryText = "<query text>";
        var outerCtx = Neo4jTransactionalContextFactory.create(() -> graph, transactionFactory)
                .newContext(outerTx, queryText, MapValue.EMPTY, QueryExecutionConfiguration.DEFAULT_CONFIG);
        var executingQuery = outerCtx.executingQuery();

        // When
        var innerCtx = outerCtx.contextWithNewTransaction();

        // Then
        assertThat(executingQuery).isSameAs(innerCtx.executingQuery());
        assertThat(executingQuery.rawQueryText()).isEqualTo(queryText);
        var snapshot = executingQuery.snapshot();
        assertThat(snapshot.transactionId())
                .isEqualTo(outerTx.kernelTransaction().getTransactionSequenceNumber());
    }

    @Test
    void contextWithNewTransactionExecutingQueryShouldSumUpPageHitsFaultsFromInnerAndOuterTransaction() {
        // Given

        // Add data to database so that we get page hits/faults
        databaseAPI.executeTransactionally("CREATE (n)");

        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var outerCtx = createTransactionContext(outerTx);
        var executingQuery = outerCtx.executingQuery();

        // When
        generatePageCacheHits(outerCtx);
        var outerHits = getPageCacheHits(outerCtx);
        var outerFaults = getPageCacheFaults(outerCtx);

        var innerCtx = outerCtx.contextWithNewTransaction();

        generatePageCacheHits(innerCtx);
        var innerHits = getPageCacheHits(innerCtx);
        var innerFaults = getPageCacheFaults(innerCtx);

        // Then
        var snapshot = executingQuery.snapshot();
        // Actual assertion
        assertThat(snapshot.pageHits()).isEqualTo(outerHits + innerHits);
        assertThat(snapshot.pageFaults()).isEqualTo(outerFaults + innerFaults);
    }

    @Test
    void
            contextWithNewTransactionExecutingQueryShouldSumUpPageHitsFaultsFromInnerAndOuterTransactionsAlsoWhenCommitted() {
        // Given

        // Add data to database so that we get page hits/faults
        var numHitsForCreateNode = createNodeAndRecordNumberOfPageHits();

        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var outerCtx = createTransactionContext(outerTx);
        var executingQuery = outerCtx.executingQuery();

        // When
        // Generate some page cache hits/faults
        generatePageCacheHits(outerCtx);
        var outerHits = getPageCacheHits(outerCtx);
        var outerFaults = getPageCacheFaults(outerCtx);

        var closedInnerHits = 0L;
        var closedInnerFaults = 0L;
        var expectedInnerCommitHits = 0L;
        var numInnerContexts = 10;
        for (int i = 0; i < numInnerContexts; i++) {
            var innerCtx = outerCtx.contextWithNewTransaction();
            //  Generate page cache hits/faults for half of the executions
            if (i % 2 == 0) {
                generatePageCacheHits(innerCtx);
                generatePageCacheHitsOnCommit(innerCtx);
                expectedInnerCommitHits += numHitsForCreateNode;
            }
            closedInnerHits += getPageCacheHits(innerCtx);
            closedInnerFaults += getPageCacheFaults(innerCtx);
            innerCtx.commit();
        }

        var openInnerCtx = outerCtx.contextWithNewTransaction();
        // Generate page cache hits/faults
        generatePageCacheHits(openInnerCtx);
        var openInnerHits = getPageCacheHits(openInnerCtx);
        var openInnerFaults = getPageCacheFaults(openInnerCtx);

        // Then
        var snapshot = executingQuery.snapshot();
        // Actual assertion
        var pageHits = snapshot.pageHits();
        assertThat(pageHits).isEqualTo(outerHits + closedInnerHits + openInnerHits + expectedInnerCommitHits);
        assertThat(snapshot.pageFaults()).isEqualTo(outerFaults + closedInnerFaults + openInnerFaults);
        assertThat(pageHits).isGreaterThanOrEqualTo(numInnerContexts / 2);
    }

    @Test
    void
            contextWithNewTransactionKernelStatisticsProviderShouldOnlySeePageHitsFaultsFromCurrentTransactionsAndInnerTransactionCommitsInPROFILE() {
        // Given

        // Add data to database so that we get page hits/faults
        var numHitsForCreateNode = createNodeAndRecordNumberOfPageHits();

        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var outerCtx = createTransactionContext(outerTx);

        // When
        // Generate some page cache hits/faults
        generatePageCacheHits(outerCtx);
        var outerHits = getPageCacheHits(outerCtx);
        var outerFaults = getPageCacheFaults(outerCtx);
        var expectedInnerCommitHits = 0L;
        var numInnerContexts = 10;
        for (int i = 0; i < numInnerContexts; i++) {
            var innerCtx = outerCtx.contextWithNewTransaction();
            //  Generate page cache hits/faults for half of the executions
            if (i % 2 == 0) {
                generatePageCacheHits(innerCtx);
                generatePageCacheHitsOnCommit(innerCtx);
                expectedInnerCommitHits += numHitsForCreateNode;
            }
            innerCtx.commit();
        }

        var openInnerCtx = outerCtx.contextWithNewTransaction();
        // Generate page cache hits/faults
        generatePageCacheHits(openInnerCtx);
        var openInnerHits = getPageCacheHits(openInnerCtx);
        var openInnerFaults = getPageCacheFaults(openInnerCtx);

        // Then
        var outerProfileStatisticsProvider = outerCtx.kernelStatisticProvider();
        var innerProfileStatisticsProvider = openInnerCtx.kernelStatisticProvider();
        // Actual assertion
        assertThat(outerProfileStatisticsProvider.getPageCacheHits()).isEqualTo(outerHits + expectedInnerCommitHits);
        assertThat(outerProfileStatisticsProvider.getPageCacheMisses()).isEqualTo(outerFaults);

        // getPageCacheHits/Misses should include statistics from the commit phase from previously closed transactions.
        // That's a little bit weird, but it works for profiling.
        var pageHits = innerProfileStatisticsProvider.getPageCacheHits();
        assertThat(pageHits).isEqualTo(openInnerHits + expectedInnerCommitHits);
        assertThat(innerProfileStatisticsProvider.getPageCacheMisses()).isEqualTo(openInnerFaults);
        assertThat(pageHits).isGreaterThanOrEqualTo(numInnerContexts / 2);
    }

    @Test
    void
            contextWithNewTransactionExecutingQueryShouldSumUpPageHitsFaultsFromInnerAndOuterTransactionsAlsoWhenRolledBack() {
        // Given

        // Add data to database so that we get page hits/faults
        databaseAPI.executeTransactionally("CREATE (n)");

        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var outerCtx = createTransactionContext(outerTx);
        var executingQuery = outerCtx.executingQuery();

        // When
        // Generate some page cache hits/faults
        generatePageCacheHits(outerCtx);
        var outerHits = getPageCacheHits(outerCtx);
        var outerFaults = getPageCacheFaults(outerCtx);

        var closedInnerHits = 0L;
        var closedInnerFaults = 0L;
        for (int i = 0; i < 10; i++) {
            var innerCtx = outerCtx.contextWithNewTransaction();
            //  Generate page cache hits/faults for half of the executions
            if (i % 2 == 0) {
                generatePageCacheHits(innerCtx);
            }
            closedInnerHits += getPageCacheHits(innerCtx);
            closedInnerFaults += getPageCacheFaults(innerCtx);
            innerCtx.rollback();
        }

        var openInnerCtx = outerCtx.contextWithNewTransaction();
        // Generate page cache hits/faults
        generatePageCacheHits(openInnerCtx);
        var openInnerHits = getPageCacheHits(openInnerCtx);
        var openInnerFaults = getPageCacheFaults(openInnerCtx);

        // Then
        var snapshot = executingQuery.snapshot();
        // Actual assertion
        assertThat(snapshot.pageHits()).isEqualTo(outerHits + closedInnerHits + openInnerHits);
        assertThat(snapshot.pageFaults()).isEqualTo(outerFaults + closedInnerFaults + openInnerFaults);
    }

    @Test
    void contextWithNewTransactionExecutingQueryShouldSumUpActiveLocksFromOpenInnerAndOuterTransactions() {
        // Given

        // Add data to database so that we can get locks
        databaseAPI.executeTransactionally("CREATE (:A), (:B), (:C)");

        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var outerCtx = createTransactionContext(outerTx);
        var executingQuery = outerCtx.executingQuery();

        // When
        // Get some locks
        getLocks(outerCtx, "A");
        var outerActiveLocks = getActiveLockCount(outerCtx);

        // First inner tx
        var openInnerCtx1 = outerCtx.contextWithNewTransaction();
        // Get some locks
        getLocks(openInnerCtx1, "B");
        var innerActiveLocks1 = getActiveLockCount(openInnerCtx1);

        // Second inner tx
        var openInnerCtx2 = outerCtx.contextWithNewTransaction();
        // Get some locks
        getLocks(openInnerCtx2, "C");
        var innerActiveLocks2 = getActiveLockCount(openInnerCtx2);

        // Then
        var snapshot = executingQuery.snapshot();
        // Make sure we are not just summing up 0s
        assertThat(outerActiveLocks).isGreaterThan(0L);
        assertThat(innerActiveLocks1).isGreaterThan(0L);
        assertThat(innerActiveLocks2).isGreaterThan(0L);
        // Actual assertion
        assertThat(snapshot.activeLockCount()).isEqualTo(outerActiveLocks + innerActiveLocks1 + innerActiveLocks2);
    }

    @Test
    void
            contextWithNewTransactionExecutingQueryShouldSumUpActiveLocksFromOpenInnerAndOuterTransactionsButNotFromClosedTransactions() {
        // Given

        // Add data to database so that we get page hits/faults
        databaseAPI.executeTransactionally("CREATE (:A), (:B), (:C), (:D)");

        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var outerCtx = createTransactionContext(outerTx);
        var executingQuery = outerCtx.executingQuery();

        // When
        getLocks(outerCtx, "A");
        var outerActiveLocks = getActiveLockCount(outerCtx);

        // Abort
        var innerCtxAbort = outerCtx.contextWithNewTransaction();
        // Get some locks
        getLocks(innerCtxAbort, "B");
        var closedInnerActiveLocksAbort = getActiveLockCount(innerCtxAbort);
        innerCtxAbort.rollback();

        // Commit
        var innerCtxCommit = outerCtx.contextWithNewTransaction();
        // Get some locks
        getLocks(innerCtxCommit, "C");
        var closedInnerActiveLocksCommit = getActiveLockCount(innerCtxCommit);
        innerCtxCommit.commit();

        // Leave open
        var innerCtxOpen = outerCtx.contextWithNewTransaction();
        // Get some locks
        getLocks(innerCtxOpen, "D");
        var openInnerActiveLocks = getActiveLockCount(innerCtxOpen);

        // Then
        var snapshot = executingQuery.snapshot();
        // Make sure we are not just summing up 0s
        assertThat(outerActiveLocks).isGreaterThan(0L);
        assertThat(closedInnerActiveLocksAbort).isGreaterThan(0L);
        assertThat(closedInnerActiveLocksCommit).isGreaterThan(0L);
        assertThat(openInnerActiveLocks).isGreaterThan(0L);
        // Actual assertion
        assertThat(snapshot.activeLockCount()).isEqualTo(outerActiveLocks + openInnerActiveLocks);
    }

    @Test
    void
            contextWithNewTransactionExecutingQueryShouldCalculateHighWaterMarkMemoryUsageAlsoWhenCommittedInQuerySnapshot() {
        // Given
        var openHighWaterMark = 3L;
        var outerHighWaterMark = 10L;

        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var outerTxMemoryTracker = outerTx.kernelTransaction().memoryTracker();
        var outerCtx = createTransactionContext(outerTx);
        var executingQuery = outerCtx.executingQuery();
        var queryMemoryTracker = QueryMemoryTracker.apply(MEMORY_TRACKING.instance());
        var outerTxMemoryTrackerForOperatorProvider =
                queryMemoryTracker.newMemoryTrackerForOperatorProvider(outerTxMemoryTracker);

        // We allocate memory through the same operator id, so it is easy for us to calculate how much memory the
        // GrowingArray of MemoryTrackerPerOperator takes
        var operatorId = 0;
        var localMem = new LocalMemoryTracker();
        var ga = HeapTrackingArrayList.newArrayList(localMem);
        ga.add(new Object());
        var growingArraySize = localMem.heapHighWaterMark();

        // Start query execution
        executingQuery.onObfuscatorReady(QueryObfuscator.PASSTHROUGH);
        executingQuery.onCompilationCompleted(null, null);
        executingQuery.onExecutionStarted(queryMemoryTracker);

        // Some operator in outer transaction allocates some memory
        outerTxMemoryTrackerForOperatorProvider
                .memoryTrackerForOperator(operatorId)
                .allocateHeap(outerHighWaterMark);

        // When
        var innerHighWaterMark = 0L;
        for (int i = 0; i < 10; i++) {
            TransactionalContext innerCtx = outerCtx.contextWithNewTransaction();
            var innerTxMemoryTracker = innerCtx.kernelTransaction().memoryTracker();
            var innerTxMemoryTrackerForOperatorProvider =
                    queryMemoryTracker.newMemoryTrackerForOperatorProvider(innerTxMemoryTracker);
            var operatorMemoryTracker = innerTxMemoryTrackerForOperatorProvider.memoryTrackerForOperator(operatorId);
            // Inner transaction allocates some memory for half of the executions
            var accHighWaterMark = 0;
            if (i % 2 == 0) {
                operatorMemoryTracker.allocateHeap(i);
                operatorMemoryTracker.releaseHeap(i);
                accHighWaterMark = i;
            }
            innerCtx.commit();
            innerHighWaterMark = Math.max(innerHighWaterMark, accHighWaterMark);
        }

        var openCtx = outerCtx.contextWithNewTransaction();
        var openTxMemoryTracker = openCtx.kernelTransaction().memoryTracker();
        var innerTxMemoryTrackerForOperatorProvider =
                queryMemoryTracker.newMemoryTrackerForOperatorProvider(openTxMemoryTracker);
        innerTxMemoryTrackerForOperatorProvider
                .memoryTrackerForOperator(operatorId)
                .allocateHeap(openHighWaterMark);

        // Then
        var snapshot = executingQuery.snapshot();
        var snapshotBytes = snapshot.allocatedBytes();
        var profilingBytes = queryMemoryTracker.heapHighWaterMark();
        assertThat(snapshotBytes)
                .isEqualTo(growingArraySize + outerHighWaterMark + Math.max(innerHighWaterMark, openHighWaterMark));
        assertThat(profilingBytes).isEqualTo(snapshotBytes);
    }

    @Test
    void contextWithNewTransactionThrowsAfterTransactionTerminate() {
        // Given
        var tx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(tx);

        // When
        tx.kernelTransaction().markForTermination(Status.Transaction.Terminated);

        // Then
        assertThrows(TransactionTerminatedException.class, ctx::contextWithNewTransaction);
    }

    @Test
    void contextWithNewTransactionThrowsAfterTransactionTerminateRace()
            throws ExecutionException, InterruptedException {
        KernelTransactions ktxs = graph.getDependencyResolver().resolveDependency(KernelTransactions.class);
        try (OtherThreadExecutor otherThreadExecutor = new OtherThreadExecutor("")) {
            for (int i = 0; i < 100; i++) {
                try (var tx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED)) {
                    var ctx = createTransactionContext(tx);

                    // When
                    BinaryLatch latch = new BinaryLatch();
                    Future<Object> future = otherThreadExecutor.executeDontWait(() -> {
                        latch.release();
                        tx.kernelTransaction().markForTermination(Status.Transaction.Terminated);
                        return null;
                    });

                    latch.await();
                    try {
                        TransactionalContext newContext = ctx.contextWithNewTransaction();
                        // If we succeed to create a new context before the termination, just close it and try again
                        newContext.transaction().close();
                        newContext.close();
                    } catch (TransactionTerminatedException e) {
                        // Since we terminate the outer tx, this is expected
                    } finally {
                        future.get();
                        ctx.close();
                    }
                }
                // Then
                assertThat(ktxs.getNumberOfActiveTransactions()).isZero();
            }
        }
    }

    @Test
    void contextWithNewTransactionTerminateInnerTransactionOnOuterTransactionTerminate() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        ctx.kernelTransaction().markForTermination(Status.Transaction.Terminated);

        // Then
        assertTrue(isMarkedForTermination(innerCtx));
    }

    @Test
    void contextWithNewTransactionDeregisterInnerTransactionOnInnerContextCommit() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        innerCtx.commit();

        // Then
        assertFalse(hasInnerTransaction(ctx));
    }

    @Test
    void contextWithNewTransactionDeregisterInnerTransactionOnInnerContextRollback() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        innerCtx.rollback();

        // Then
        assertFalse(hasInnerTransaction(ctx));
    }

    @Test
    void contextWithNewTransactionDeregisterInnerTransactionOnInnerContextClose() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        innerCtx.transaction().close();

        // Then
        assertFalse(hasInnerTransaction(ctx));
    }

    private boolean hasInnerTransaction(TransactionalContext ctx) {
        KernelTransactions kernelTransactions =
                graph.getDependencyResolver().resolveDependency(KernelTransactions.class);
        KernelTransaction kernelTransaction = ctx.kernelTransaction();
        long transactionCountOnCurrentQuery = kernelTransactions.executingTransactions().stream()
                .flatMap(handle -> handle.executingQuery().stream()
                        .map(ExecutingQuery::snapshot)
                        .map(QuerySnapshot::transactionId)
                        .filter(txnId -> txnId == kernelTransaction.getTransactionSequenceNumber()))
                .count();
        return transactionCountOnCurrentQuery > 1;
    }

    @Test
    void contextWithNewTransactionThrowIfInnerTransactionPresentOnOuterTransactionCommit() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();

        ctx.close();

        // Then
        assertThrows(
                TransactionFailureException.class,
                // When
                ctx::commit);
    }

    @Test
    void contextWithNewTransactionDoesNotThrowIfInnerTransactionDeregisteredOnOuterTransactionCommit() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();
        var innerTx = innerCtx.transaction();

        innerCtx.commit();
        ctx.close();

        // Then
        assertDoesNotThrow(
                // When
                ctx::commit);
    }

    @Test
    void contextWithNewTransactionThrowOnRollbackOfTransactionWithInnerTransactions() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();

        // Then
        assertThrows(
                TransactionFailureException.class,
                // When
                outerTx::rollback);
    }

    @Test
    void contextWithNewTransactionThrowOnCloseOfTransactionWithInnerTransactions() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();

        // Then
        assertThrows(
                TransactionFailureException.class,
                // When
                outerTx::close);
    }

    @Test
    void contextWithNewTransactionDoNotTerminateOuterTransactionOnInnerTransactionTerminate() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        innerCtx.kernelTransaction().markForTermination(Status.Transaction.Terminated);

        // Then
        assertFalse(isMarkedForTermination(ctx));
    }

    @Test
    void contextWithNewTransactionDoNotCloseOuterContextOnInnerContextRollback() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        innerCtx.rollback();

        // Then
        assertTrue(ctx.isOpen());
    }

    @Test
    void contextWithNewTransactionCloseInnerStatementOnInnerContextCommitClose() throws Exception {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var outerCtx = createTransactionContext(outerTx);
        var innerCtx = outerCtx.contextWithNewTransaction();

        var outerCloseable = mock(AutoCloseable.class);
        var innerCloseable = mock(AutoCloseable.class);

        outerCtx.statement().registerCloseableResource(outerCloseable);
        innerCtx.statement().registerCloseableResource(innerCloseable);

        // When
        innerCtx.commit();

        // Then
        verify(innerCloseable).close();
        verifyNoMoreInteractions(innerCloseable);
        verifyNoInteractions(outerCloseable);
    }

    @Test
    void contextWithNewTransactionCloseInnerStatementOnInnerTransactionCommitClose() throws Exception {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var outerCtx = createTransactionContext(outerTx);
        var innerCtx = outerCtx.contextWithNewTransaction();
        var innerTx = innerCtx.transaction();

        var outerCloseable = mock(AutoCloseable.class);
        var innerCloseable = mock(AutoCloseable.class);

        outerCtx.statement().registerCloseableResource(outerCloseable);
        innerCtx.statement().registerCloseableResource(innerCloseable);

        // Then (we close the transaction w/o closing the referencing context)
        assertThrows(
                TransactionFailureException.class,
                // When
                innerTx::commit);
        verify(innerCloseable).close();
        verifyNoMoreInteractions(innerCloseable);
        verifyNoInteractions(outerCloseable);
    }

    @Test
    void contextWithNewTransactionProcedureCalledFromInnerContextShouldUseInnerTransaction() throws ProcedureException {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();

        var procsRegistry = databaseAPI.getDependencyResolver().resolveDependency(GlobalProcedures.class);
        var txSetMetaData =
                procsRegistry.getCurrentView().procedure(new QualifiedName(new String[] {"tx"}, "setMetaData"));
        var id = txSetMetaData.id();
        var procContext = new ProcedureCallContext(id, EMPTY_STRING_ARRAY, false, "", false, "runtimeUsed");

        // When
        AnyValue[] arguments = {VirtualValues.map(new String[] {"foo"}, new AnyValue[] {Values.stringValue("bar")})};
        innerCtx.kernelTransaction().procedures().procedureCallDbms(id, arguments, procContext);

        // Then
        assertThat(innerCtx.kernelTransaction().getMetaData()).isEqualTo(Collections.singletonMap("foo", "bar"));
        assertThat(ctx.kernelTransaction().getMetaData()).isEqualTo(Collections.emptyMap());
    }

    @Test
    void contextWithNewTransactionListTransactions() throws InvalidArgumentsException {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var queryText = "<query text>";
        var ctx = Neo4jTransactionalContextFactory.create(() -> graph, transactionFactory)
                .newContext(outerTx, queryText, MapValue.EMPTY, QueryExecutionConfiguration.DEFAULT_CONFIG);

        // We need to be done with parsing and provide an obfuscator to see the query text in the procedure
        ctx.executingQuery().onObfuscatorReady(QueryObfuscator.PASSTHROUGH);

        var innerCtx = ctx.contextWithNewTransaction();
        var innerTx = innerCtx.transaction();

        // show the transactions (discarding the SHOW TRANSACTIONS transaction)
        var transactions =
                innerTx.execute("SHOW TRANSACTIONS WHERE NOT currentQuery STARTS WITH 'SHOW TRANSACTIONS'").stream()
                        .toList();

        assertThat(transactions.size()).isEqualTo(1);

        // When
        var transactionId = transactions.get(0).get("transactionId");
        var currentQuery = transactions.get(0).get("currentQuery");
        var currentQueryId = transactions.get(0).get("currentQueryId");

        // Then
        var expectedTransactionId = TransactionId.formatTransactionId(
                outerTx.getDatabaseName(), outerTx.kernelTransaction().getTransactionSequenceNumber());
        var expectedQueryId = String.format("query-%s", ctx.executingQuery().id());

        assertThat(transactionId).isEqualTo(expectedTransactionId);
        assertThat(currentQuery).isEqualTo(queryText);
        assertThat(currentQueryId).isEqualTo(expectedQueryId);
    }

    @Test
    void contextWithNewTransactionKillQuery() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var queryText = "<query text>";
        var ctx = Neo4jTransactionalContextFactory.create(() -> graph, transactionFactory)
                .newContext(outerTx, queryText, MapValue.EMPTY, QueryExecutionConfiguration.DEFAULT_CONFIG);

        // We need to be done with parsing and provide an obfuscator to see the query text in the procedure
        ctx.executingQuery().onObfuscatorReady(QueryObfuscator.PASSTHROUGH);

        var innerCtx = ctx.contextWithNewTransaction();
        var innerTx = innerCtx.transaction();

        // When
        var userTransactionId = ctx.kernelTransaction().getTransactionSequenceNumber();

        // we are forcing the TERMINATE TRANSACTION to execute to completion
        // so that we can be ready to make assertions on the terminationReason
        //noinspection ResultOfMethodCallIgnored
        innerTx.execute("TERMINATE TRANSACTION 'neo4j-transaction-" + userTransactionId + "'").stream()
                .toList();

        // Then
        assertTrue(innerTx.terminationReason().isPresent());
        assertTrue(outerTx.terminationReason().isPresent());
    }

    @Test
    void contextWithRestartedTransactionShouldSumUpPageHitsFaultsFromFirstAndSecondTransactionInQuerySnapshot() {
        // Given

        // Add data to database so that we get page hits/faults
        databaseAPI.executeTransactionally("CREATE (n)");

        var transaction = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(transaction);
        var executingQuery = ctx.executingQuery();

        // When
        var closedTxHits = 0L;
        var closedTxFaults = 0L;
        for (int i = 0; i < 10; i++) {
            //  Generate page cache hits/faults for half of the executions
            if (i % 2 == 0) {
                generatePageCacheHits(ctx);
            }
            closedTxHits += getPageCacheHits(ctx);
            closedTxFaults += getPageCacheFaults(ctx);
            ctx.commitAndRestartTx();
        }

        generatePageCacheHits(ctx);

        var lastHits = getPageCacheHits(ctx);
        var lastFaults = getPageCacheFaults(ctx);
        var lastTx = ctx.transaction();

        // Then
        var snapshot = executingQuery.snapshot();
        // Actual assertion
        assertThat(snapshot.transactionId())
                .isEqualTo(lastTx.kernelTransaction().getTransactionSequenceNumber());
        assertThat(snapshot.pageHits()).isEqualTo(closedTxHits + lastHits);
        assertThat(snapshot.pageFaults()).isEqualTo(closedTxFaults + lastFaults);
    }

    @Test
    void contextWithRestartedTransactionShouldSumUpPageHitsFaultsFromFirstAndSecondTransactionInPROFILE() {
        // Given

        // Add data to database so that we get page hits/faults
        databaseAPI.executeTransactionally("CREATE (n)");

        var transaction = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(transaction);

        var closedTxHits = 0L;
        var closedTxFaults = 0L;
        for (int i = 0; i < 10; i++) {
            //  Generate page cache hits/faults for half of the executions
            if (i % 2 == 0) {
                generatePageCacheHits(ctx);
            }
            closedTxHits += getPageCacheHits(ctx);
            closedTxFaults += getPageCacheFaults(ctx);
            ctx.commitAndRestartTx();
        }

        generatePageCacheHits(ctx);

        var lastHits = getPageCacheHits(ctx);
        var lastFaults = getPageCacheFaults(ctx);

        // Then
        var profileStatisticsProvider = ctx.kernelStatisticProvider();
        // Actual assertion
        assertThat(profileStatisticsProvider.getPageCacheHits()).isEqualTo(closedTxHits + lastHits);
        assertThat(profileStatisticsProvider.getPageCacheMisses()).isEqualTo(closedTxFaults + lastFaults);
    }

    @Test
    void restartingContextDoesNotLeakKernelTransaction() {
        InternalTransaction transaction = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var transactionContext = createTransactionContext(transaction);

        var kernelTransactions = graph.getDependencyResolver().resolveDependency(KernelTransactions.class);
        int initialActiveCount = kernelTransactions.getNumberOfActiveTransactions();

        for (int i = 0; i < 1024; i++) {
            transactionContext.commitAndRestartTx();
            // we check with some offset to make sure any background job will not fail assertion
            assertThat(kernelTransactions.getNumberOfActiveTransactions()).isCloseTo(initialActiveCount, offset(5));
        }
    }

    @Test
    void contextWithRestartedTransactionShouldReuseExecutingQuery() {
        // Given

        var internalTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var firstKernelTx = internalTx.kernelTransaction();
        var ctx = createTransactionContext(internalTx);
        var firstStatement = ctx.statement();
        //noinspection OptionalGetWithoutIsPresent
        var firstExecutingQuery = ((KernelStatement) firstStatement)
                .queryRegistry()
                .executingQuery()
                .get();

        // When
        ctx.commitAndRestartTx();

        // Then
        var secondKernelTx = internalTx.kernelTransaction();
        var secondStatement = ctx.statement();
        //noinspection OptionalGetWithoutIsPresent
        var secondExecutingQuery = ((KernelStatement) secondStatement)
                .queryRegistry()
                .executingQuery()
                .get();

        assertThat(secondKernelTx).isNotSameAs(firstKernelTx);
        assertThat(secondStatement).isNotSameAs(firstStatement);
        assertThat(secondExecutingQuery).isSameAs(firstExecutingQuery);
        assertFalse(firstKernelTx.isOpen());
    }

    @Test
    void contextWithNewTransactionsQueryTransactionShouldReuseExecutingQuery() {
        // Given

        var firstInternalTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var firstKernelTx = firstInternalTx.kernelTransaction();
        var ctx = createTransactionContext(firstInternalTx);
        var firstStatement = ctx.statement();
        //noinspection OptionalGetWithoutIsPresent
        var firstExecutingQuery = ((KernelStatement) firstStatement)
                .queryRegistry()
                .executingQuery()
                .get();

        // When
        ctx = ctx.contextWithNewTransaction();

        // Then
        var secondInternalTx = ctx.transaction();
        var secondKernelTx = secondInternalTx.kernelTransaction();
        var secondStatement = ctx.statement();
        //noinspection OptionalGetWithoutIsPresent
        var secondExecutingQuery = ((KernelStatement) secondStatement)
                .queryRegistry()
                .executingQuery()
                .get();

        assertThat(secondInternalTx).isNotSameAs(firstInternalTx);
        assertThat(secondKernelTx).isNotSameAs(firstKernelTx);
        assertThat(secondStatement).isNotSameAs(firstStatement);
        assertThat(secondExecutingQuery).isSameAs(firstExecutingQuery);
        assertTrue(firstKernelTx.isOpen());
    }

    @Test
    void shouldBeAbleToAccessExecutingQueryWhileCommitting() {
        // Given
        KernelTransactions kernelTransactions =
                graph.getDependencyResolver().resolveDependency(KernelTransactions.class);
        BinaryLatch inCommitLatch = new BinaryLatch();
        BinaryLatch releaseCommitLatch = new BinaryLatch();
        dbms.registerTransactionEventListener(databaseAPI.databaseName(), new TransactionEventListenerAdapter<>() {

            @Override
            public Object beforeCommit(
                    TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
                    throws Exception {
                inCommitLatch.release();
                releaseCommitLatch.await();
                return super.beforeCommit(data, transaction, databaseService);
            }
        });

        // When

        try (OtherThreadExecutor executor = new OtherThreadExecutor("test")) {
            AtomicReference<QueryRegistry> reg = new AtomicReference<>();
            executor.executeDontWait(() -> {
                var internalTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
                var ctx = createTransactionContext(internalTx);
                internalTx.execute("CREATE (n)");
                reg.set(((KernelStatement) ctx.statement()).queryRegistry());
                ctx.commit();
                return null;
            });
            try {
                inCommitLatch.await();

                // Then
                var txs = kernelTransactions.executingTransactions();
                assertThat(txs).hasSize(1);
                assertThat(txs.iterator().next().executingQuery()).isPresent();
                assertThat(reg.get().executingQuery()).isPresent();
            } finally {
                releaseCommitLatch.release();
            }
        }
    }
}
