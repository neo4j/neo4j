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
package org.neo4j.kernel.impl.query;

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
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.config.MEMORY_TRACKING;
import org.neo4j.cypher.internal.runtime.memory.QueryMemoryTracker;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
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
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.builtin.QueryId;
import org.neo4j.procedure.builtin.TransactionId;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

@ImpermanentDbmsExtension
class Neo4jTransactionalContextIT {
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
                .newContext(transaction, "no query", EMPTY_MAP);
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
                .newContext(outerTx, queryText, MapValue.EMPTY);
        var executingQuery = outerCtx.executingQuery();

        // When
        var innerCtx = outerCtx.contextWithNewTransaction();

        // Then
        assertThat(executingQuery).isSameAs(innerCtx.executingQuery());
        assertThat(executingQuery.rawQueryText()).isEqualTo(queryText);
        var snapshot = executingQuery.snapshot();
        assertThat(snapshot.transactionId())
                .isEqualTo(outerTx.kernelTransaction().getUserTransactionId());
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
            innerCtx.close();
            innerCtx.transaction().commit();
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
    void
            contextWithNewTransactionKernelStatisticsProviderShouldOnlySeePageHitsFaultsFromCurrentTransactionsInPROFILE() {
        // Given

        // Add data to database so that we get page hits/faults
        databaseAPI.executeTransactionally("CREATE (n)");

        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var outerCtx = createTransactionContext(outerTx);

        // When
        // Generate some page cache hits/faults
        generatePageCacheHits(outerCtx);
        var outerHits = getPageCacheHits(outerCtx);
        var outerFaults = getPageCacheFaults(outerCtx);

        for (int i = 0; i < 10; i++) {
            var innerCtx = outerCtx.contextWithNewTransaction();
            //  Generate page cache hits/faults for half of the executions
            if (i % 2 == 0) {
                generatePageCacheHits(innerCtx);
            }
            innerCtx.close();
            innerCtx.transaction().commit();
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
        assertThat(outerProfileStatisticsProvider.getPageCacheHits()).isEqualTo(outerHits);
        assertThat(outerProfileStatisticsProvider.getPageCacheMisses()).isEqualTo(outerFaults);
        assertThat(innerProfileStatisticsProvider.getPageCacheHits()).isEqualTo(openInnerHits);
        assertThat(innerProfileStatisticsProvider.getPageCacheMisses()).isEqualTo(openInnerFaults);
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
        innerCtxCommit.close();
        innerCtxCommit.transaction().commit();

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
            innerCtx.close();
            innerCtx.transaction().commit();
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
        innerCtx.close();
        innerCtx.transaction().commit();

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
                        .filter(txnId -> txnId == kernelTransaction.getUserTransactionId()))
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
                outerTx::commit);
    }

    @Test
    void contextWithNewTransactionDoesNotThrowIfInnerTransactionDeregisteredOnOuterTransactionCommit() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();
        var innerTx = innerCtx.transaction();

        innerCtx.close();
        innerTx.commit();
        ctx.close();

        // Then
        assertDoesNotThrow(
                // When
                outerTx::commit);
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

    @Disabled(
            "Strictly speaking this does not need to work, but it would protect us from our own programming mistakes in Cypher")
    @Test
    void contextWithNewTransactionCloseInnerContextOnOuterContextRollback() {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();

        // When
        ctx.rollback();

        // Then
        assertFalse(innerCtx.isOpen());
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
        innerCtx.close();
        innerCtx.transaction().commit();

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
    void contextWithNewTransactionShouldThrowIfOuterTransactionIsExplicit() {
        // Given
        var outerTx = graph.beginTransaction(EXPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);

        // Then
        //noinspection Convert2MethodRef
        assertThrows(
                TransactionFailureException.class,
                // When
                () -> ctx.contextWithNewTransaction());
    }

    @Test
    void contextWithNewTransactionProcedureCalledFromInnerContextShouldUseInnerTransaction() throws ProcedureException {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var ctx = createTransactionContext(outerTx);
        var innerCtx = ctx.contextWithNewTransaction();

        var procsRegistry = databaseAPI.getDependencyResolver().resolveDependency(GlobalProcedures.class);
        var txSetMetaData = procsRegistry.procedure(new QualifiedName(new String[] {"tx"}, "setMetaData"));
        var id = txSetMetaData.id();
        var procContext = new ProcedureCallContext(id, new String[0], false, "", false);

        // When
        AnyValue[] arguments = {VirtualValues.map(new String[] {"foo"}, new AnyValue[] {Values.stringValue("bar")})};
        innerCtx.kernelTransaction().procedures().procedureCallDbms(id, arguments, procContext);

        // Then
        assertThat(innerCtx.kernelTransaction().getMetaData()).isEqualTo(Collections.singletonMap("foo", "bar"));
        assertThat(ctx.kernelTransaction().getMetaData()).isEqualTo(Collections.emptyMap());
    }

    @Test
    void contextWithNewTransactionListTransactions() throws ProcedureException, InvalidArgumentsException {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var queryText = "<query text>";
        var ctx = Neo4jTransactionalContextFactory.create(() -> graph, transactionFactory)
                .newContext(outerTx, queryText, MapValue.EMPTY);

        // We need to be done with parsing and provide an obfuscator to see the query text in the procedure
        ctx.executingQuery().onObfuscatorReady(QueryObfuscator.PASSTHROUGH);

        var innerCtx = ctx.contextWithNewTransaction();
        var innerTx = innerCtx.transaction();

        var procsRegistry = databaseAPI.getDependencyResolver().resolveDependency(GlobalProcedures.class);
        var listTransactions = procsRegistry.procedure(new QualifiedName(new String[] {"dbms"}, "listTransactions"));
        var id = listTransactions.id();
        var transactionIdIndex = listTransactions
                .signature()
                .outputSignature()
                .indexOf(FieldSignature.outputField("transactionId", Neo4jTypes.NTString));
        var currentQueryIndex = listTransactions
                .signature()
                .outputSignature()
                .indexOf(FieldSignature.outputField("currentQuery", Neo4jTypes.NTString));
        var currentQueryIdIndex = listTransactions
                .signature()
                .outputSignature()
                .indexOf(FieldSignature.outputField("currentQueryId", Neo4jTypes.NTString));
        var outerTransactionIdIndex = listTransactions
                .signature()
                .outputSignature()
                .indexOf(FieldSignature.outputField("outerTransactionId", Neo4jTypes.NTString));
        var procContext = new ProcedureCallContext(
                id,
                new String[] {"transactionId", "currentQuery", "currentQueryId", "outerTransactionId"},
                false,
                "",
                false);

        // When
        var procResult = Iterators.asList(
                innerCtx.kernelTransaction().procedures().procedureCallDbms(id, new AnyValue[] {}, procContext));

        var mapper = new DefaultValueMapper(innerTx);
        var transactionIds = procResult.stream()
                .map(array -> array[transactionIdIndex].map(mapper))
                .toList();
        var currentQueries = procResult.stream()
                .map(array -> array[currentQueryIndex].map(mapper))
                .toList();
        var currentQueryIds = procResult.stream()
                .map(array -> array[currentQueryIdIndex].map(mapper))
                .toList();
        var outerTransactionIds = procResult.stream()
                .map(array -> array[outerTransactionIdIndex].map(mapper))
                .toList();

        // Then
        var expectedOuterTxId = new TransactionId(
                        outerTx.getDatabaseName(), outerTx.kernelTransaction().getUserTransactionId())
                .toString();
        var expectedInnerTxId = new TransactionId(
                        innerTx.getDatabaseName(), innerTx.kernelTransaction().getUserTransactionId())
                .toString();
        var expectedQueryId = String.format("query-%s", ctx.executingQuery().id());

        assertThat(transactionIds).contains(expectedOuterTxId, expectedInnerTxId);
        assertThat(transactionIds).hasSize(2);
        assertThat(currentQueries).contains(queryText, queryText);
        assertThat(currentQueries).hasSize(2);
        assertThat(currentQueryIds).contains(expectedQueryId, expectedQueryId);
        assertThat(currentQueryIds).hasSize(2);
        assertThat(outerTransactionIds).contains(expectedOuterTxId, "");
        assertThat(outerTransactionIds).hasSize(2);
    }

    @Test
    void contextWithNewTransactionListQueries() throws ProcedureException, InvalidArgumentsException {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var queryText = "<query text>";
        var ctx = Neo4jTransactionalContextFactory.create(() -> graph, transactionFactory)
                .newContext(outerTx, queryText, MapValue.EMPTY);

        // We need to be done with parsing and provide an obfuscator to see the query text in the procedure
        ctx.executingQuery().onObfuscatorReady(QueryObfuscator.PASSTHROUGH);

        var innerCtx = ctx.contextWithNewTransaction();
        var innerTx = innerCtx.transaction();

        var procsRegistry = databaseAPI.getDependencyResolver().resolveDependency(GlobalProcedures.class);
        var listQueries = procsRegistry.procedure(new QualifiedName(new String[] {"dbms"}, "listQueries"));
        var procedureId = listQueries.id();
        var transactionIdIndex = listQueries
                .signature()
                .outputSignature()
                .indexOf(FieldSignature.outputField("transactionId", Neo4jTypes.NTString));
        var queryIndex = listQueries
                .signature()
                .outputSignature()
                .indexOf(FieldSignature.outputField("query", Neo4jTypes.NTString));
        var queryIdIndex = listQueries
                .signature()
                .outputSignature()
                .indexOf(FieldSignature.outputField("queryId", Neo4jTypes.NTString));
        var procContext = new ProcedureCallContext(
                procedureId, new String[] {"transactionId", "query", "queryId"}, false, "", false);

        // When
        var procResult = Iterators.asList(innerCtx.kernelTransaction()
                .procedures()
                .procedureCallDbms(procedureId, new AnyValue[] {}, procContext));

        var mapper = new DefaultValueMapper(innerTx);
        var transactionIds = procResult.stream()
                .map(array -> array[transactionIdIndex].map(mapper))
                .toList();
        var queries =
                procResult.stream().map(array -> array[queryIndex].map(mapper)).toList();
        var queryIds = procResult.stream()
                .map(array -> array[queryIdIndex].map(mapper))
                .toList();

        // Then
        var expectedTransactionId = new TransactionId(
                        outerTx.getDatabaseName(), outerTx.kernelTransaction().getUserTransactionId())
                .toString();
        var expectedQueryId = String.format("query-%s", ctx.executingQuery().id());

        assertThat(transactionIds).isEqualTo(Collections.singletonList(expectedTransactionId));
        assertThat(queries).isEqualTo(Collections.singletonList(queryText));
        assertThat(queryIds).isEqualTo(Collections.singletonList(expectedQueryId));
    }

    @Test
    void contextWithNewTransactionKillQuery() throws ProcedureException, InvalidArgumentsException {
        // Given
        var outerTx = graph.beginTransaction(IMPLICIT, LoginContext.AUTH_DISABLED);
        var queryText = "<query text>";
        var ctx = Neo4jTransactionalContextFactory.create(() -> graph, transactionFactory)
                .newContext(outerTx, queryText, MapValue.EMPTY);

        // We need to be done with parsing and provide an obfuscator to see the query text in the procedure
        ctx.executingQuery().onObfuscatorReady(QueryObfuscator.PASSTHROUGH);

        var innerCtx = ctx.contextWithNewTransaction();
        var innerTx = innerCtx.transaction();

        var procedureRegistry = databaseAPI.getDependencyResolver().resolveDependency(GlobalProcedures.class);
        var listQueries = procedureRegistry.procedure(new QualifiedName(new String[] {"dbms"}, "killQuery"));
        var procedureId = listQueries.id();
        var procContext = new ProcedureCallContext(procedureId, new String[] {}, false, "", false);

        // When
        TextValue argument = Values.stringValue(new QueryId(ctx.executingQuery().internalQueryId()).toString());
        innerCtx.kernelTransaction()
                .procedures()
                .procedureCallDbms(procedureId, new AnyValue[] {argument}, procContext);

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
                .isEqualTo(lastTx.kernelTransaction().getUserTransactionId());
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
}
