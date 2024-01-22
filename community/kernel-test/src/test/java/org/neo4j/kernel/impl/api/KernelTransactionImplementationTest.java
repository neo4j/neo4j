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
package org.neo4j.kernel.impl.api;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionTimedOutClientConfiguration;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionValidationFailed;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.api.TransactionTimeout;
import org.neo4j.kernel.api.database.enrichment.TxEnrichmentVisitor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.WriteOnReadOnlyAccessDbException;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.kernel.impl.monitoring.TransactionMonitor;
import org.neo4j.lock.LockTracer;
import org.neo4j.memory.MemoryLimitExceededException;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.resources.CpuClock;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.enrichment.EnrichmentMode;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.test.DoubleLatch;
import org.neo4j.util.concurrent.Futures;

class KernelTransactionImplementationTest extends KernelTransactionTestBase {

    private static final TransactionTimeout DEFAULT_TX_TIMEOUT =
            new TransactionTimeout(Duration.ZERO, TransactionTimedOutClientConfiguration);

    private static Stream<Arguments> parameters() {
        Consumer<KernelTransaction> readTxInitializer = tx -> {};
        Consumer<KernelTransaction> writeTxInitializer =
                tx -> ((KernelTransactionImplementation) tx).txState().nodeDoCreate(42);
        return Stream.of(
                arguments("readOperationsInNewTransaction", false, readTxInitializer),
                arguments("write", true, writeTxInitializer));
    }

    private static Stream<Arguments> enrichmentModes() {
        return Stream.of(
                Arguments.of(EnrichmentMode.OFF, Type.IMPLICIT, Boolean.FALSE),
                Arguments.of(EnrichmentMode.DIFF, Type.IMPLICIT, Boolean.FALSE),
                Arguments.of(EnrichmentMode.FULL, Type.IMPLICIT, Boolean.FALSE),
                Arguments.of(EnrichmentMode.OFF, Type.IMPLICIT, Boolean.TRUE),
                Arguments.of(EnrichmentMode.DIFF, Type.IMPLICIT, Boolean.TRUE),
                Arguments.of(EnrichmentMode.FULL, Type.IMPLICIT, Boolean.TRUE),
                Arguments.of(EnrichmentMode.OFF, Type.EXPLICIT, Boolean.TRUE),
                Arguments.of(EnrichmentMode.DIFF, Type.EXPLICIT, Boolean.TRUE),
                Arguments.of(EnrichmentMode.FULL, Type.EXPLICIT, Boolean.TRUE));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void changeTransactionTracingWithoutRestart(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        try (KernelTransactionImplementation transaction = newTransaction(loginContext(isWriteTx))) {
            // WHEN
            transactionInitializer.accept(transaction);
            assertSame(TransactionInitializationTrace.NONE, transaction.getInitializationTrace());
        }
        config.setDynamic(
                GraphDatabaseSettings.transaction_tracing_level,
                GraphDatabaseSettings.TransactionTracingLevel.ALL,
                getClass().getSimpleName());
        try (KernelTransactionImplementation transaction = newTransaction(loginContext(isWriteTx))) {
            // WHEN
            transactionInitializer.accept(transaction);
            assertNotSame(TransactionInitializationTrace.NONE, transaction.getInitializationTrace());
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void emptyMetadataReturnedWhenMetadataIsNotSet(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        try (KernelTransaction transaction = newTransaction(loginContext(isWriteTx))) {
            Map<String, Object> metaData = transaction.getMetaData();
            assertTrue(metaData.isEmpty());
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void accessSpecifiedTransactionMetadata(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        try (KernelTransaction transaction = newTransaction(loginContext(isWriteTx))) {
            Map<String, Object> externalMetadata = map("Robot", "Bender", "Human", "Fry");
            transaction.setMetaData(externalMetadata);
            Map<String, Object> transactionMetadata = transaction.getMetaData();
            assertFalse(transactionMetadata.isEmpty());
            assertEquals("Bender", transactionMetadata.get("Robot"));
            assertEquals("Fry", transactionMetadata.get("Human"));
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldCommitSuccessfulTransaction(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        // GIVEN
        try (KernelTransaction transaction = newTransaction(loginContext(isWriteTx))) {
            // WHEN
            transactionInitializer.accept(transaction);
            transaction.commit();
        }

        // THEN
        verify(transactionMonitor).transactionFinished(true, isWriteTx);
        verifyTransactionSizeInteractionWithMonitor();
        verifyExtraInteractionWithTheMonitor(transactionMonitor, isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldRollbackUnsuccessfulTransaction(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        // GIVEN
        try (KernelTransaction transaction = newTransaction(loginContext(isWriteTx))) {
            // WHEN
            transactionInitializer.accept(transaction);
        }

        // THEN
        verify(transactionMonitor).transactionFinished(false, isWriteTx);
        verifyExtraInteractionWithTheMonitor(transactionMonitor, isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldRollbackFailedTransaction(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        // GIVEN
        try (KernelTransaction transaction = newTransaction(loginContext(isWriteTx))) {
            // WHEN
            transactionInitializer.accept(transaction);
            transaction.rollback();
        }

        // THEN
        verify(transactionMonitor).transactionFinished(false, isWriteTx);
        verifyExtraInteractionWithTheMonitor(transactionMonitor, isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldRollbackAndThrowOnFailedAndSuccess(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) {
        // GIVEN
        assertThrows(NotInTransactionException.class, () -> {
            try (KernelTransaction transaction = newTransaction(loginContext(isWriteTx))) {
                // WHEN
                transactionInitializer.accept(transaction);
                transaction.rollback();
                transaction.commit();
            }
        });
        verify(transactionMonitor).transactionFinished(false, isWriteTx);
        verifyExtraInteractionWithTheMonitor(transactionMonitor, isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldRollbackOnClosingTerminatedTransaction(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) {
        // GIVEN
        KernelTransaction transaction = newTransaction(loginContext(isWriteTx));

        transactionInitializer.accept(transaction);
        transaction.markForTermination(Status.General.UnknownError);

        assertThrows(TransactionTerminatedException.class, transaction::commit);

        // THEN
        verify(transactionMonitor).transactionFinished(false, isWriteTx);
        verify(transactionMonitor).transactionTerminated(isWriteTx);
        verifyExtraInteractionWithTheMonitor(transactionMonitor, isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldRollbackOnClosingSuccessfulButTerminatedTransaction(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        try (KernelTransaction transaction = newTransaction(loginContext(isWriteTx))) {
            // WHEN
            transactionInitializer.accept(transaction);
            transaction.markForTermination(Status.General.UnknownError);
            assertEquals(
                    Status.General.UnknownError,
                    transaction.getReasonIfTerminated().get());
        }

        // THEN
        verify(transactionMonitor).transactionFinished(false, isWriteTx);
        verify(transactionMonitor).transactionTerminated(isWriteTx);
        verifyExtraInteractionWithTheMonitor(transactionMonitor, isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldRollbackOnClosingTerminatedButSuccessfulTransaction(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) {
        // GIVEN
        KernelTransaction transaction = newTransaction(loginContext(isWriteTx));

        transactionInitializer.accept(transaction);
        transaction.markForTermination(Status.General.UnknownError);
        assertEquals(
                Status.General.UnknownError, transaction.getReasonIfTerminated().get());

        assertThrows(TransactionTerminatedException.class, transaction::commit);

        // THEN
        verify(transactionMonitor).transactionFinished(false, isWriteTx);
        verify(transactionMonitor).transactionTerminated(isWriteTx);
        verifyExtraInteractionWithTheMonitor(transactionMonitor, isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldNotDowngradeFailureState(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        try (KernelTransaction transaction = newTransaction(loginContext(isWriteTx))) {
            // WHEN
            transactionInitializer.accept(transaction);
            transaction.markForTermination(Status.General.UnknownError);
            assertEquals(
                    Status.General.UnknownError,
                    transaction.getReasonIfTerminated().get());
        }

        // THEN
        verify(transactionMonitor).transactionFinished(false, isWriteTx);
        verify(transactionMonitor).transactionTerminated(isWriteTx);
        verifyExtraInteractionWithTheMonitor(transactionMonitor, isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldIgnoreTerminateAfterCommit(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        KernelTransaction transaction = newTransaction(loginContext(isWriteTx));
        transactionInitializer.accept(transaction);
        transaction.commit();
        transaction.markForTermination(Status.General.UnknownError);

        // THEN
        verify(transactionMonitor).transactionFinished(true, isWriteTx);
        verifyTransactionSizeInteractionWithMonitor();
        verifyExtraInteractionWithTheMonitor(transactionMonitor, isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldIgnoreTerminateAfterRollback(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        KernelTransaction transaction = newTransaction(loginContext(isWriteTx));
        transactionInitializer.accept(transaction);
        transaction.close();
        transaction.markForTermination(Status.General.UnknownError);

        // THEN
        verify(transactionMonitor).transactionFinished(false, isWriteTx);
        verifyExtraInteractionWithTheMonitor(transactionMonitor, isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldThrowOnTerminationInCommit(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) {
        KernelTransaction transaction = newTransaction(loginContext(isWriteTx));
        transactionInitializer.accept(transaction);
        transaction.markForTermination(Status.General.UnknownError);

        assertThrows(TransactionTerminatedException.class, transaction::commit);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldIgnoreTerminationDuringRollback(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        KernelTransaction transaction = newTransaction(loginContext(isWriteTx));
        transactionInitializer.accept(transaction);
        transaction.markForTermination(Status.General.UnknownError);
        transaction.close();

        // THEN
        verify(transactionMonitor).transactionFinished(false, isWriteTx);
        verify(transactionMonitor).transactionTerminated(isWriteTx);
        verifyExtraInteractionWithTheMonitor(transactionMonitor, isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldAllowTerminatingFromADifferentThread(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch(1);
        final KernelTransaction transaction = newTransaction(loginContext(isWriteTx));
        transactionInitializer.accept(transaction);

        var executorService = Executors.newSingleThreadExecutor();
        try {
            Future<?> terminationFuture = executorService.submit(() -> {
                latch.waitForAllToStart();
                transaction.markForTermination(Status.General.UnknownError);
                latch.finish();
            });

            // WHEN
            latch.startAndWaitForAllToStartAndFinish();

            assertNull(terminationFuture.get(1, TimeUnit.MINUTES));
            assertThrows(TransactionTerminatedException.class, transaction::commit);
        } finally {
            executorService.shutdownNow();
        }

        // THEN
        verify(transactionMonitor).transactionFinished(false, isWriteTx);
        verify(transactionMonitor).transactionTerminated(isWriteTx);
        verifyExtraInteractionWithTheMonitor(transactionMonitor, isWriteTx);
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("parameters")
    void shouldUseStartTimeAndTxIdFromWhenStartingTxAsHeader(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        // GIVEN a transaction starting at one point in time
        long startingTime = clock.millis();
        when(storageEngine.createCommands(
                        any(TransactionState.class),
                        any(StorageReader.class),
                        any(CommandCreationContext.class),
                        any(LockTracer.class),
                        any(TxStateVisitor.Decorator.class),
                        any(CursorContext.class),
                        any(StoreCursors.class),
                        any(MemoryTracker.class)))
                .thenReturn(List.of(mock(StorageCommand.class)));

        try (KernelTransactionImplementation transaction = newTransaction(loginContext(isWriteTx))) {
            transaction.initialize(
                    5L,
                    KernelTransaction.Type.IMPLICIT,
                    SecurityContext.AUTH_DISABLED,
                    DEFAULT_TX_TIMEOUT,
                    1L,
                    EMBEDDED_CONNECTION,
                    mock(ProcedureView.class));
            transaction.txState().nodeDoCreate(1L);
            // WHEN committing it at a later point
            clock.forward(5, MILLISECONDS);
            // ...and simulating some other transaction being committed
            when(metadataProvider.getLastCommittedTransactionId()).thenReturn(7L);
            transaction.commit();
        }

        // THEN start time and last tx when started should have been taken from when the transaction started
        assertEquals(5L, getObservedFirstTransaction().getLatestCommittedTxWhenStarted());
        assertEquals(startingTime, getObservedFirstTransaction().getTimeStarted());
        assertEquals(startingTime + 5, getObservedFirstTransaction().getTimeCommitted());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void successfulTxShouldNotifyKernelTransactionsThatItIsClosed(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        KernelTransactionImplementation tx = newTransaction(loginContext(isWriteTx));

        tx.close();

        verify(txPool).release(tx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void failedTxShouldNotifyKernelTransactionsThatItIsClosed(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        KernelTransactionImplementation tx = newTransaction(loginContext(isWriteTx));

        tx.close();

        verify(txPool).release(tx);
    }

    private void verifyExtraInteractionWithTheMonitor(TransactionMonitor transactionMonitor, boolean isWriteTx) {
        if (isWriteTx) {
            verify(this.transactionMonitor).upgradeToWriteTransaction();
        }
        verifyNoMoreInteractions(transactionMonitor);
    }

    private void verifyTransactionSizeInteractionWithMonitor() {
        verify(transactionMonitor).addHeapTransactionSize(anyLong());
        verify(transactionMonitor).addNativeTransactionSize(anyLong());
    }

    @Test
    void markForTerminationNotInitializedTransaction() {
        KernelTransactionImplementation tx = newNotInitializedTransaction();
        tx.markForTermination(Status.General.UnknownError);

        assertThat(tx.getReasonIfTerminated()).isEmpty();
    }

    @Test
    void newNotInitializedTransactionIsNotOpen() {
        KernelTransactionImplementation tx = newNotInitializedTransaction();
        assertThat(tx.isOpen()).isFalse();
    }

    @Test
    void reusedNotInitializedTransactionIsNotOpen() throws TransactionFailureException {
        KernelTransactionImplementation tx = newTransaction(loginContext(false));
        tx.close();
        verify(txPool).release(eq(tx));

        // We pretend we just got this tx from the pool
        assertThat(tx.isOpen()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void markForTerminationInitializedTransaction(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) {
        KernelTransactionImplementation tx = newTransaction(loginContext(isWriteTx));

        tx.markForTermination(Status.General.UnknownError);

        assertEquals(Status.General.UnknownError, tx.getReasonIfTerminated().get());
        verify(locksClient).stop();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void markForTerminationTerminatedTransaction(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) {
        KernelTransactionImplementation tx = newTransaction(loginContext(isWriteTx));
        transactionInitializer.accept(tx);

        tx.markForTermination(Status.Transaction.Terminated);
        tx.markForTermination(Status.Transaction.Outdated);
        tx.markForTermination(Status.Transaction.LockClientStopped);

        assertEquals(Status.Transaction.Terminated, tx.getReasonIfTerminated().get());
        verify(locksClient).stop();
        verify(transactionMonitor).transactionTerminated(isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void terminatedTxThrowsOnCommit(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) {
        KernelTransactionImplementation tx = newTransaction(loginContext(isWriteTx));
        transactionInitializer.accept(tx);
        tx.markForTermination(Status.General.UnknownError);

        assertThrows(TransactionTerminatedException.class, tx::commit);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void terminatedTxMarkedForFailureClosesWithoutThrowing(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        KernelTransactionImplementation tx = newTransaction(loginContext(isWriteTx));
        transactionInitializer.accept(tx);
        tx.markForTermination(Status.General.UnknownError);

        tx.close();

        verify(locksClient).stop();
        verify(transactionMonitor).transactionTerminated(isWriteTx);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void initializedTransactionShouldHaveNoTerminationReason(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) {
        KernelTransactionImplementation tx = newTransaction(loginContext(isWriteTx));
        assertFalse(tx.getReasonIfTerminated().isPresent());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldReportCorrectTerminationReason(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) {
        Status status = Status.Transaction.Terminated;
        KernelTransactionImplementation tx = newTransaction(loginContext(isWriteTx));
        tx.markForTermination(status);
        assertSame(status, tx.getReasonIfTerminated().get());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void closedTransactionShouldHaveNoTerminationReason(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        KernelTransactionImplementation tx = newTransaction(loginContext(isWriteTx));
        tx.markForTermination(Status.Transaction.Terminated);
        tx.close();
        assertFalse(tx.getReasonIfTerminated().isPresent());
    }

    @Test
    void transactionWithCustomTimeout() {
        long transactionTimeout = 5L;
        KernelTransactionImplementation transaction = newTransaction(transactionTimeout);
        assertEquals(
                new TransactionTimeout(Duration.ofMillis(transactionTimeout), TransactionTimedOutClientConfiguration),
                transaction.timeout(),
                "Transaction should have custom configured timeout.");
    }

    @Test
    void transactionStartTime() {
        long startTime = clock.forward(5, TimeUnit.MINUTES).millis();
        KernelTransactionImplementation transaction = newTransaction(AUTH_DISABLED);
        assertEquals(startTime, transaction.startTime(), "Transaction start time should be the same as clock time.");
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void markForTerminationWithCorrectUserTxId(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        long userTransactionId = 10;
        Status.Transaction terminationReason = Status.Transaction.Terminated;

        KernelTransactionImplementation tx = newTransaction(2L, AUTH_DISABLED, DEFAULT_TX_TIMEOUT, userTransactionId);

        assertTrue(tx.markForTermination(userTransactionId, terminationReason));

        assertEquals(terminationReason, tx.getReasonIfTerminated().get());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void markForTerminationWithIncorrectUserTxId(
            String name, boolean isWriteTx, Consumer<KernelTransaction> transactionInitializer) throws Exception {
        long userTransactionId = 13;
        long wrongUserTransactionId = userTransactionId + 2;
        Status.Transaction terminationReason = Status.Transaction.Terminated;

        KernelTransactionImplementation tx = newTransaction(2L, AUTH_DISABLED, DEFAULT_TX_TIMEOUT, userTransactionId);

        assertFalse(tx.markForTermination(wrongUserTransactionId, terminationReason));

        assertFalse(tx.getReasonIfTerminated().isPresent());
    }

    @Test
    void resetTransactionStatisticsOnRelease() throws TransactionFailureException {
        KernelTransactionImplementation transaction = newTransaction(1000);
        transaction.getStatistics().addWaitingTime(1);
        transaction.getStatistics().addWaitingTime(1);
        assertEquals(2, transaction.getStatistics().getWaitingTimeNanos(0));
        transaction.close();
        assertEquals(0, transaction.getStatistics().getWaitingTimeNanos(0));
    }

    @Test
    void reportTransactionStatistics() {
        KernelTransactionImplementation transaction = newTransaction(100);
        transaction.memoryTracker().allocateHeap(13);
        transaction.memoryTracker().allocateNative(14);
        KernelTransactionImplementation.Statistics statistics = new KernelTransactionImplementation.Statistics(
                transaction, new AtomicReference<>(new ThreadBasedCpuClock()), false);
        PredictablePageCursorTracer tracer = new PredictablePageCursorTracer();
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        statistics.init(2);

        assertEquals(2, statistics.cpuTimeMillis());
        assertEquals(mebiBytes(2), statistics.estimatedHeapMemory());
        assertEquals(14, statistics.usedNativeMemory());
        assertEquals(-1, statistics.heapAllocatedBytes());
        statistics.addWaitingTime(1);
        assertEquals(1, statistics.getWaitingTimeNanos(0));

        transaction.memoryTracker().releaseNative(14);
        statistics.reset();
        transaction.memoryTracker().reset();

        statistics.init(4);
        assertEquals(4, statistics.cpuTimeMillis());
        assertEquals(0, statistics.estimatedHeapMemory());
        assertEquals(0, statistics.usedNativeMemory());
        assertEquals(-1, statistics.heapAllocatedBytes());
        assertEquals(0, statistics.getWaitingTimeNanos(0));
    }

    @Test
    void includeLeaseInToString() {
        int leaseId = 11;
        LeaseService leaseClient = mock(LeaseService.class);
        when(leaseClient.newClient()).thenReturn(new LeaseClient() {
            @Override
            public int leaseId() {
                return leaseId;
            }

            @Override
            public void ensureValid() throws LeaseException {}
        });
        KernelTransactionImplementation transaction = newNotInitializedTransaction(leaseClient);
        transaction.initialize(
                0,
                KernelTransaction.Type.IMPLICIT,
                mock(SecurityContext.class),
                DEFAULT_TX_TIMEOUT,
                1L,
                EMBEDDED_CONNECTION,
                mock(ProcedureView.class));
        assertEquals("KernelTransaction[lease:" + leaseId + "]", transaction.toString());
    }

    @Test
    void shouldThrowWhenCreatingTxStateWithInvalidLease() {
        // given
        var leaseService = mock(LeaseService.class);
        when(leaseService.newClient()).thenReturn(new LeaseClient() {
            @Override
            public int leaseId() {
                return 0;
            }

            @Override
            public void ensureValid() throws LeaseException {
                throw new LeaseException("Invalid lease!", TransactionValidationFailed);
            }
        });
        var transaction = newNotInitializedTransaction(leaseService);
        transaction.initialize(
                0,
                Type.IMPLICIT,
                mock(SecurityContext.class),
                DEFAULT_TX_TIMEOUT,
                1L,
                EMBEDDED_CONNECTION,
                mock(ProcedureView.class));

        // when / then
        assertThrows(LeaseException.class, transaction::txState);
    }

    @Test
    void shouldThrowWhenCreatingTxStateWithReadOnlyDatabase() {
        // given
        var fooName = "foo";
        var fooDb = DatabaseIdFactory.from(fooName, UUID.randomUUID());
        var configValues = Map.of(read_only_database_default, false, read_only_databases, Set.of(fooName));
        var config = Config.defaults(configValues);

        var transaction = newNotInitializedTransaction(config, fooDb);
        transaction.initialize(
                0,
                Type.IMPLICIT,
                mock(SecurityContext.class),
                DEFAULT_TX_TIMEOUT,
                1L,
                EMBEDDED_CONNECTION,
                mock(ProcedureView.class));

        // when / then
        var rte = assertThrows(RuntimeException.class, transaction::txState);
        assertThat(rte).hasCauseInstanceOf(WriteOnReadOnlyAccessDbException.class);
    }

    @Test
    void shouldDeregisterConfigListenersOnDispose() {
        // given
        var config = spy(Config.defaults());

        var transaction = newNotInitializedTransaction(config);
        transaction.initialize(
                0,
                Type.IMPLICIT,
                mock(SecurityContext.class),
                DEFAULT_TX_TIMEOUT,
                1L,
                EMBEDDED_CONNECTION,
                mock(ProcedureView.class));

        verify(config, times(3)).addListener(any(), any());

        // when / then
        transaction.dispose();

        verify(config, times(3)).removeListener(any(), any());
    }

    @Test
    void dynamicChangeTransactionHeapLimit() throws TransactionFailureException {
        config.set(memory_transaction_max_size, mebiBytes(2));
        try (KernelTransactionImplementation transaction = newTransaction(1000)) {
            // Limit should prevent this from succeeding
            assertThrows(
                    MemoryLimitExceededException.class,
                    () -> transaction.memoryTracker().allocateHeap(mebiBytes(3)));
            transaction.closeTransaction();

            // Increase limit and try again
            config.setDynamic(memory_transaction_max_size, mebiBytes(4), "test");
            transaction.initialize(
                    5L,
                    Type.IMPLICIT,
                    SecurityContext.AUTH_DISABLED,
                    DEFAULT_TX_TIMEOUT,
                    1L,
                    EMBEDDED_CONNECTION,
                    mock(ProcedureView.class));

            transaction.memoryTracker().allocateHeap(mebiBytes(3));
        }
    }

    @Test
    void transactionExecutionContexts() throws TransactionFailureException, ExecutionException, InterruptedException {
        int workerCount = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(workerCount);

        try (var transaction = newTransaction(AUTH_DISABLED);
                var statement = transaction.acquireStatement()) {
            List<ExecutionContext> executionContexts = new ArrayList<>(workerCount);
            List<Future<?>> futures = new ArrayList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                executionContexts.add(transaction.createExecutionContext());
            }
            for (int i = 0; i < workerCount; i++) {
                ExecutionContext executionContext = executionContexts.get(i);
                int iterations = i;
                futures.add(executorService.submit(() -> {
                    try {
                        PageCursorTracer cursorTracer =
                                executionContext.cursorContext().getCursorTracer();
                        for (int j = 0; j <= iterations; j++) {
                            PageSwapper swapper = mock(PageSwapper.class, RETURNS_MOCKS);
                            try (var pinEvent = cursorTracer.beginPin(false, 1, swapper)) {
                                try (var pageFaultEvent = pinEvent.beginPageFault(1, swapper)) {
                                    pageFaultEvent.addBytesRead(42);
                                }
                            }
                            cursorTracer.unpin(1, swapper);
                        }
                    } finally {
                        executionContext.complete();
                    }
                }));
            }
            Futures.getAll(futures);
            closeAllUnchecked(executionContexts);

            PageCursorTracer transactionCursor = transaction.cursorContext().getCursorTracer();

            assertEquals(10, transactionCursor.pins());
            assertEquals(10, transactionCursor.unpins());
            assertEquals(420, transactionCursor.bytesRead());
        } finally {
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(1, MINUTES));
        }
    }

    @ParameterizedTest
    @MethodSource("enrichmentModes")
    void txStateShouldGetEnrichmentModeFromStorageEngine(EnrichmentMode mode, Type txType, boolean createNode)
            throws Exception {
        // GIVEN a transaction starting at one point in time
        when(serverIdentity.serverId()).thenReturn(new ServerId(UUID.randomUUID()));
        when(enrichmentStrategy.check()).thenReturn(mode);
        @SuppressWarnings("resource")
        final var stateVisitor = mock(TxStateVisitor.class);

        final var storeReader = mock(StorageReader.class);
        when(storeReader.constraintsGetAll()).thenReturn(Collections.emptyIterator());

        when(storageEngine.newReader()).thenReturn(storeReader);
        when(storageEngine.createCommands(
                        any(TransactionState.class),
                        any(StorageReader.class),
                        any(CommandCreationContext.class),
                        any(LockTracer.class),
                        any(TxStateVisitor.Decorator.class),
                        any(CursorContext.class),
                        any(StoreCursors.class),
                        any(MemoryTracker.class)))
                .thenAnswer(args -> {
                    final TxStateVisitor.Decorator decorator = args.getArgument(4);
                    final var commandVisitor = decorator.apply(stateVisitor);
                    if (mode != EnrichmentMode.OFF && createNode) {
                        assertThat(commandVisitor).isInstanceOf(TxEnrichmentVisitor.class);
                    } else {
                        assertThat(commandVisitor).isNotInstanceOf(TxEnrichmentVisitor.class);
                    }
                    return List.of(mock(StorageCommand.class));
                });

        try (KernelTransactionImplementation transaction = newTransaction(AnonymousContext.write())) {
            transaction.initialize(
                    5L,
                    txType,
                    SecurityContext.AUTH_DISABLED,
                    DEFAULT_TX_TIMEOUT,
                    1L,
                    EMBEDDED_CONNECTION,
                    mock(ProcedureView.class));
            assertThat(((TxState) transaction.txState()).enrichmentMode()).isEqualTo(mode);
            if (createNode) {
                transaction.dataWrite().nodeCreate();
            } else {
                transaction.tokenWrite().labelCreateForName("label", false);
            }
            transaction.commit();
        }
    }

    @Test
    void shouldNotLooseExceptionInCommit() {
        KernelTransactionImplementation transaction = newTransaction(AUTH_DISABLED);
        transaction.txState().nodeDoCreate(5);
        RuntimeException foo = new RuntimeException("foo");
        RuntimeException bar = new RuntimeException("bar");
        doThrow(foo).when(locksClient).prepareForCommit();
        doThrow(bar).when(locksClient).close();
        assertThatThrownBy(transaction::commit).isSameAs(foo).hasSuppressedException(bar);
    }

    @Test
    void shouldNotLooseExceptionInCommitWhenRollbackIsFailed() {
        KernelTransactionImplementation transaction = newTransaction(AUTH_DISABLED);
        transaction.txState().nodeDoCreate(5);
        RuntimeException foo = new RuntimeException("foo");
        RuntimeException bar = new RuntimeException("bar");
        doThrow(foo).when(transactionValidator).validate(anyCollection(), anyLong(), any(), any(), any());
        doThrow(bar).when(storageEngine).release(any(), any(), any(), anyBoolean());
        assertThatThrownBy(transaction::commit).isSameAs(foo).hasSuppressedException(bar);
    }

    @Test
    void shouldNotCheckLeaseOnSuccessfulTx() {
        KernelTransactionImplementation transaction = newNotInitializedTransaction(new ExpiredLeases());
        initialize(
                0,
                AUTH_DISABLED,
                new TransactionTimeout(Duration.ofSeconds(1), TransactionTimedOutClientConfiguration),
                1L,
                transaction);
        assertThatCode(transaction::commit).doesNotThrowAnyException();
    }

    @Test
    void shouldCheckLeaseOnUnexpectedException() {
        ExpiredLeases leases = new ExpiredLeases();
        KernelTransactionImplementation transaction = newNotInitializedTransaction(leases);
        initialize(
                0,
                AUTH_DISABLED,
                new TransactionTimeout(Duration.ofSeconds(1), TransactionTimedOutClientConfiguration),
                1L,
                transaction);
        RuntimeException foo = new RuntimeException("foo");
        doThrow(foo).when(locksClient).close();
        assertThatThrownBy(transaction::commit).isSameAs(foo).hasSuppressedException(leases.expired());
    }

    @Test
    void shouldResetEverythingOnException() {
        KernelTransactionImplementation transaction = newTransaction(AUTH_DISABLED);
        transaction.txState().nodeDoCreate(5);
        RuntimeException foo = new RuntimeException("foo");

        assertThat(transaction.getInnerTransactionHandler()).isNotNull();
        doThrow(foo).when(locksClient).close();
        assertThatThrownBy(transaction::close).isSameAs(foo);
        assertThatThrownBy(transaction::getInnerTransactionHandler).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldReleaseIfSuccesfulCleanup() throws TransactionFailureException {
        KernelTransactionImplementation transaction = newTransaction(AUTH_DISABLED);
        transaction.txState().nodeDoCreate(5);

        transaction.close();

        verify(txPool, times(1)).release(transaction);
    }

    @Test
    void shouldDisposeIfFailedCleanup() {
        KernelTransactionImplementation transaction = newTransaction(AUTH_DISABLED);
        transaction.txState().nodeDoCreate(5);
        RuntimeException foo = new RuntimeException("foo");

        doThrow(foo).when(locksClient).close();
        assertThatThrownBy(transaction::close).isSameAs(foo);

        verify(txPool, never()).release(transaction);
        verify(txPool, times(1)).dispose(transaction);
    }

    private static LoginContext loginContext(boolean isWriteTx) {
        return isWriteTx ? AnonymousContext.write() : AnonymousContext.read();
    }

    private CommandBatch getObservedFirstTransaction() {
        return commitProcess.transactions.get(0);
    }

    private static class ThreadBasedCpuClock implements CpuClock {
        private long iteration;

        @Override
        public long cpuTimeNanos(long threadId) {
            iteration++;
            return MILLISECONDS.toNanos(iteration * threadId);
        }
    }

    private static class PredictablePageCursorTracer extends DefaultPageCursorTracer {
        private long iteration = 1;

        PredictablePageCursorTracer() {
            super(new DefaultPageCacheTracer(), "ktxTest");
        }

        @Override
        public long hits() {
            iteration++;
            return iteration * 2;
        }

        @Override
        public long faults() {
            return iteration;
        }
    }

    private static class ExpiredLeases implements LeaseService {
        LeaseException expired() {
            return new LeaseException("Expired", Status.Cluster.NotALeader);
        }

        @Override
        public LeaseClient newClient() {
            return new LeaseClient() {
                @Override
                public int leaseId() {
                    return 0;
                }

                @Override
                public void ensureValid() throws LeaseException {
                    throw expired();
                }
            };
        }
    }
}
