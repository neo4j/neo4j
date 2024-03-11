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
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;
import static org.neo4j.kernel.api.TransactionTimeout.NO_TIMEOUT;
import static org.neo4j.kernel.api.security.AnonymousContext.access;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier.ON_HEAP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.txstate.validation.TransactionValidatorFactory.EMPTY_VALIDATOR_FACTORY;
import static org.neo4j.util.concurrent.Futures.combine;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.monitoring.TransactionMonitor;
import org.neo4j.kernel.impl.query.TransactionExecutionMonitor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.tracing.DefaultTracers;
import org.neo4j.lock.LockTracer;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.resources.CpuClock;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.enrichment.ApplyEnrichmentStrategy;
import org.neo4j.storageengine.api.enrichment.EnrichmentMode;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThread;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.ElementIdMapper;

@ExtendWith(OtherThreadExtension.class)
class KernelTransactionsTest {
    private static final NamedDatabaseId DEFAULT_DATABASE_ID = from(DEFAULT_DATABASE_NAME, UUID.randomUUID());
    private static final SystemNanoClock clock = Clocks.nanoClock();
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private DatabaseAvailabilityGuard databaseAvailabilityGuard;

    @Inject
    private OtherThread t2;

    private final LockManager locks = mock(LockManager.class);

    @BeforeEach
    void setUp() {
        databaseAvailabilityGuard = new DatabaseAvailabilityGuard(
                DEFAULT_DATABASE_ID, clock, NullLog.getInstance(), 0, mock(CompositeDatabaseAvailabilityGuard.class));
        databaseAvailabilityGuard.init();
    }

    @AfterEach
    void tearDown() {
        executorService.shutdownNow();
    }

    @Test
    void shouldNotAllocateSystemTransactionId() throws Throwable {
        // Given
        KernelTransactions transactions = newTestKernelTransactions();

        // When
        KernelTransaction firstTxn = getKernelTransaction(transactions);

        // Then
        assertThat(firstTxn.getTransactionSequenceNumber()).isNotEqualTo(0L);
    }

    @Test
    void shouldListActiveTransactions() throws Throwable {
        // Given
        KernelTransactions transactions = newTestKernelTransactions();

        // When
        KernelTransaction first = getKernelTransaction(transactions);
        KernelTransaction second = getKernelTransaction(transactions);
        KernelTransaction third = getKernelTransaction(transactions);

        first.close();

        // Then
        assertThat(transactions.activeTransactions()).isEqualTo(asSet(newHandle(second), newHandle(third)));
    }

    @Test
    void shouldDisposeTransactionsWhenAsked() throws Throwable {
        // Given
        KernelTransactions transactions = newKernelTransactions();

        transactions.disposeAll();

        KernelTransaction first = getKernelTransaction(transactions);
        KernelTransaction second = getKernelTransaction(transactions);
        KernelTransaction leftOpen = getKernelTransaction(transactions);
        first.close();
        second.close();

        // When
        transactions.disposeAll();

        // Then
        KernelTransaction postDispose = getKernelTransaction(transactions);
        assertThat(postDispose).isNotEqualTo(first);
        assertThat(postDispose).isNotEqualTo(second);

        assertNotNull(leftOpen.getReasonIfTerminated());
    }

    @Test
    void shouldReuseClosedTransactionObjects() throws Throwable {
        // GIVEN
        KernelTransactions transactions = newKernelTransactions();
        KernelTransaction a = getKernelTransaction(transactions);

        // WHEN
        a.close();
        KernelTransaction b = getKernelTransaction(transactions);

        // THEN
        assertSame(a, b);
    }

    @Test
    void shouldNotReuseTransactionsUnableToClose() throws Throwable {
        // GIVEN
        KernelTransactions transactions = newKernelTransactions();
        LockManager.Client client = mock(LockManager.Client.class);
        RuntimeException foo = new RuntimeException("foo");
        doThrow(foo).when(client).close();
        when(locks.newClient()).thenReturn(client);

        // WHEN
        KernelTransaction a = getKernelTransaction(transactions);

        // THEN
        assertThat(transactions.getNumberOfActiveTransactions()).isOne();
        assertThatThrownBy(a::close).isSameAs(foo);
        assertThat(transactions.getNumberOfActiveTransactions()).isZero();

        // WHEN
        KernelTransaction b = getKernelTransaction(transactions);

        // THEN
        assertNotSame(a, b);
        assertThat(transactions.getNumberOfActiveTransactions()).isOne();
    }

    @Test
    void shouldTellWhenTransactionsFromSnapshotHaveBeenClosed() throws Throwable {
        // GIVEN
        KernelTransactions transactions = newKernelTransactions();
        KernelTransaction a = getKernelTransaction(transactions);
        KernelTransaction b = getKernelTransaction(transactions);
        KernelTransaction c = getKernelTransaction(transactions);
        IdController.TransactionSnapshot snapshot = transactions.get();
        assertFalse(transactions.eligibleForFreeing(snapshot));

        // WHEN a gets closed
        a.close();
        assertFalse(transactions.eligibleForFreeing(snapshot));

        // WHEN c gets closed and (test knowing too much) that instance getting reused in another transaction "d".
        c.close();
        KernelTransaction d = getKernelTransaction(transactions);
        assertFalse(transactions.eligibleForFreeing(snapshot));

        // WHEN b finally gets closed
        b.close();
        assertTrue(transactions.eligibleForFreeing(snapshot));
    }

    @Test
    void shouldTellWhenTransactionsFromSnapshotHaveBeenTerminated() throws Throwable {
        // GIVEN
        KernelTransactions transactions = newKernelTransactions();
        KernelTransaction a = getKernelTransaction(transactions);
        KernelTransaction b = getKernelTransaction(transactions);
        KernelTransaction c = getKernelTransaction(transactions);
        IdController.TransactionSnapshot snapshot = transactions.get();
        assertFalse(transactions.eligibleForFreeing(snapshot));

        // WHEN a gets closed
        a.markForTermination(Status.Transaction.Terminated);
        assertFalse(transactions.eligibleForFreeing(snapshot));

        // WHEN c gets closed and (test knowing too much) that instance getting reused in another transaction "d".
        c.markForTermination(Status.Transaction.Terminated);
        KernelTransaction d = getKernelTransaction(transactions);
        assertFalse(transactions.eligibleForFreeing(snapshot));

        // WHEN b finally gets closed
        b.markForTermination(Status.Transaction.Terminated);
        assertTrue(transactions.eligibleForFreeing(snapshot));
    }

    @Test
    void shouldBeAbleToSnapshotDuringHeavyLoad() throws Throwable {
        // GIVEN
        final KernelTransactions transactions = newKernelTransactions();
        Race race = new Race();
        final int threads = 50;
        final AtomicBoolean end = new AtomicBoolean();
        final AtomicReferenceArray<IdController.TransactionSnapshot> snapshots = new AtomicReferenceArray<>(threads);

        // Representing "transaction" threads
        for (int i = 0; i < threads; i++) {
            final int threadIndex = i;
            race.addContestant(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while (!end.get()) {
                    try (KernelTransaction ignored = getKernelTransaction(transactions)) {
                        parkNanos(MILLISECONDS.toNanos(random.nextInt(3)));
                        if (snapshots.get(threadIndex) == null) {
                            snapshots.set(threadIndex, transactions.get());
                            parkNanos(MILLISECONDS.toNanos(random.nextInt(3)));
                        }
                    } catch (TransactionFailureException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        // Just checks snapshots
        race.addContestant(() -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            int snapshotsLeft = 1_000;
            while (snapshotsLeft > 0) {
                int threadIndex = random.nextInt(threads);
                IdController.TransactionSnapshot snapshot = snapshots.get(threadIndex);
                if (snapshot != null && transactions.eligibleForFreeing(snapshot)) {
                    snapshotsLeft--;
                    snapshots.set(threadIndex, null);
                }
            }

            // End condition of this test can be described as:
            //   when 1000 snapshots have been seen as closed.
            // setting this boolean to true will have all other threads end as well so that race.go() will end
            end.set(true);
        });

        // WHEN
        race.go();
    }

    @Test
    void transactionCloseRemovesTxFromActiveTransactions() throws Throwable {
        KernelTransactions kernelTransactions = newTestKernelTransactions();

        KernelTransaction tx1 = getKernelTransaction(kernelTransactions);
        KernelTransaction tx2 = getKernelTransaction(kernelTransactions);
        KernelTransaction tx3 = getKernelTransaction(kernelTransactions);

        tx1.close();
        tx3.close();

        assertEquals(asSet(newHandle(tx2)), kernelTransactions.activeTransactions());
    }

    @Test
    void disposeAllMarksAllTransactionsForTermination() throws Throwable {
        KernelTransactions kernelTransactions = newKernelTransactions();

        KernelTransaction tx1 = getKernelTransaction(kernelTransactions);
        KernelTransaction tx2 = getKernelTransaction(kernelTransactions);
        KernelTransaction tx3 = getKernelTransaction(kernelTransactions);

        kernelTransactions.disposeAll();

        assertEquals(
                Status.General.DatabaseUnavailable, tx1.getReasonIfTerminated().get());
        assertEquals(
                Status.General.DatabaseUnavailable, tx2.getReasonIfTerminated().get());
        assertEquals(
                Status.General.DatabaseUnavailable, tx3.getReasonIfTerminated().get());
    }

    @Test
    void transactionClosesUnderlyingStoreReaderWhenDisposed() throws Throwable {
        // Given
        StorageReader storeStatement1 = mock(StorageReader.class);
        StorageReader storeStatement2 = mock(StorageReader.class);
        StorageReader storeStatement3 = mock(StorageReader.class);
        KernelTransactions kernelTransactions = newKernelTransactions(
                mock(TransactionCommitProcess.class), storeStatement1, storeStatement2, storeStatement3);
        // And three active transactions
        var txOne = kernelTransactions.newInstance(EXPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, NO_TIMEOUT);
        var txTwo = kernelTransactions.newInstance(EXPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, NO_TIMEOUT);
        var txThree = kernelTransactions.newInstance(EXPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, NO_TIMEOUT);
        assertThat(kernelTransactions.activeTransactions().size()).isEqualTo(3);

        // When
        txOne.close();
        txTwo.close();
        txThree.close();
        kernelTransactions.disposeAll();

        // Then
        verify(storeStatement1).close();
        verify(storeStatement2).close();
        verify(storeStatement3).close();
    }

    @Test
    void threadThatBlocksNewTxsCantStartNewTxs() throws Throwable {
        KernelTransactions kernelTransactions = newKernelTransactions();
        kernelTransactions.blockNewTransactions();
        var e = assertThrows(
                Exception.class,
                () -> kernelTransactions.newInstance(
                        IMPLICIT, AnonymousContext.write(), EMBEDDED_CONNECTION, NO_TIMEOUT));
        assertThat(e).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @Timeout(10)
    void blockNewTransactions() throws Throwable {
        KernelTransactions kernelTransactions = newKernelTransactions();
        kernelTransactions.blockNewTransactions();

        Future<KernelTransaction> txOpener = t2.execute(() ->
                kernelTransactions.newInstance(EXPLICIT, AnonymousContext.write(), EMBEDDED_CONNECTION, NO_TIMEOUT));
        t2.get().waitUntilWaiting(location -> location.isAt(KernelTransactions.class, "newInstance"));

        assertNotDone(txOpener);

        kernelTransactions.unblockNewTransactions();
        assertNotNull(txOpener.get());
    }

    @Test
    @Timeout(10)
    void unblockNewTransactionsFromWrongThreadThrows() throws Throwable {
        KernelTransactions kernelTransactions = newKernelTransactions();
        kernelTransactions.blockNewTransactions();

        Future<KernelTransaction> txOpener = t2.execute(() ->
                kernelTransactions.newInstance(EXPLICIT, AnonymousContext.write(), EMBEDDED_CONNECTION, NO_TIMEOUT));
        t2.get().waitUntilWaiting(location -> location.isAt(KernelTransactions.class, "newInstance"));

        assertNotDone(txOpener);

        Future<?> wrongUnblocker = unblockTxsInSeparateThread(kernelTransactions);

        var e = assertThrows(ExecutionException.class, wrongUnblocker::get);
        assertThat(e.getCause()).isInstanceOf(IllegalStateException.class);
        assertNotDone(txOpener);

        kernelTransactions.unblockNewTransactions();
        assertNotNull(txOpener.get());
    }

    @Test
    void shouldNotLeakTransactionOnSecurityContextFreezeFailure() throws Throwable {
        KernelTransactions kernelTransactions = newKernelTransactions();
        LoginContext loginContext = mock(LoginContext.class);
        when(loginContext.authorize(any(), any(), any()))
                .thenThrow(new AuthorizationExpiredException("Freeze failed."));

        assertThatThrownBy(
                        () -> kernelTransactions.newInstance(EXPLICIT, loginContext, EMBEDDED_CONNECTION, NO_TIMEOUT))
                .isInstanceOf(AuthorizationExpiredException.class)
                .hasMessage("Freeze failed.");

        assertThat(kernelTransactions.activeTransactions())
                .as("We should not have any transaction")
                .isEmpty();
    }

    @Test
    void exceptionWhenStartingNewTransactionOnShutdownInstance() throws Throwable {
        KernelTransactions kernelTransactions = newKernelTransactions();

        databaseAvailabilityGuard.shutdown();

        assertThrows(
                DatabaseShutdownException.class,
                () -> kernelTransactions.newInstance(EXPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, NO_TIMEOUT));
    }

    @Test
    void exceptionWhenStartingNewTransactionOnStoppedKernelTransactions() throws Throwable {
        KernelTransactions kernelTransactions = newKernelTransactions();
        t2.execute(() -> {
                    stopKernelTransactions(kernelTransactions);
                    return null;
                })
                .get();

        assertThrows(
                IllegalStateException.class,
                () -> kernelTransactions.newInstance(EXPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, NO_TIMEOUT));
    }

    @Test
    void startNewTransactionOnRestartedKErnelTransactions() throws Throwable {
        KernelTransactions kernelTransactions = newKernelTransactions();

        kernelTransactions.stop();
        kernelTransactions.start();
        assertNotNull(
                kernelTransactions.newInstance(EXPLICIT, AUTH_DISABLED, EMBEDDED_CONNECTION, NO_TIMEOUT),
                "New transaction created by restarted kernel transactions component.");
    }

    @Test
    void incrementalUserTransactionId() throws Throwable {
        KernelTransactions kernelTransactions = newKernelTransactions();
        try (KernelTransaction kernelTransaction =
                kernelTransactions.newInstance(EXPLICIT, AnonymousContext.access(), EMBEDDED_CONNECTION, NO_TIMEOUT)) {
            assertEquals(
                    1, kernelTransactions.activeTransactions().iterator().next().getTransactionSequenceNumber());
        }

        try (KernelTransaction kernelTransaction =
                kernelTransactions.newInstance(EXPLICIT, AnonymousContext.access(), EMBEDDED_CONNECTION, NO_TIMEOUT)) {
            assertEquals(
                    2, kernelTransactions.activeTransactions().iterator().next().getTransactionSequenceNumber());
        }

        try (KernelTransaction kernelTransaction =
                kernelTransactions.newInstance(EXPLICIT, AnonymousContext.access(), EMBEDDED_CONNECTION, NO_TIMEOUT)) {
            assertEquals(
                    3, kernelTransactions.activeTransactions().iterator().next().getTransactionSequenceNumber());
        }
    }

    @Test
    void shouldNotAllowToCreateMoreThenMaxActiveTransactions() throws Throwable {
        Config config = Config.defaults(GraphDatabaseSettings.max_concurrent_transactions, 2);
        KernelTransactions kernelTransactions = newKernelTransactions(config);
        KernelTransaction ignore = kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT);
        KernelTransaction ignore2 = kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT);

        assertThrows(
                MaximumTransactionLimitExceededException.class,
                () -> kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT));
    }

    @Test
    void allowToBeginTransactionsWhenSlotsAvailableAgain() throws Throwable {
        Config config = Config.defaults(GraphDatabaseSettings.max_concurrent_transactions, 2);
        KernelTransactions kernelTransactions = newKernelTransactions(config);
        KernelTransaction ignore = kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT);
        KernelTransaction ignore2 = kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT);

        assertThrows(
                MaximumTransactionLimitExceededException.class,
                () -> kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT));

        ignore.close();
        // fine to start again
        kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT);
    }

    @Test
    void allowToBeginTransactionsWhenConfigChanges() throws Throwable {
        Config config = Config.defaults(GraphDatabaseSettings.max_concurrent_transactions, 2);
        KernelTransactions kernelTransactions = newKernelTransactions(config);
        KernelTransaction ignore = kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT);
        KernelTransaction ignore2 = kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT);

        assertThrows(
                MaximumTransactionLimitExceededException.class,
                () -> kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT));

        config.setDynamic(
                GraphDatabaseSettings.max_concurrent_transactions, 3, getClass().getSimpleName());

        // fine to start again
        kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT);
    }

    @Test
    void reuseSameTransactionInOneThread() throws Throwable {
        KernelTransactions kernelTransactions = newKernelTransactions();
        for (int i = 0; i < 100; i++) {
            try (KernelTransaction kernelTransaction =
                    kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT)) {
                assertEquals(1, kernelTransactions.getNumberOfActiveTransactions());
            }
        }
        assertEquals(0, kernelTransactions.getNumberOfActiveTransactions());
    }

    @Test
    void trackNumberOfActiveTransactionFromMultipleThreads() throws Throwable {
        KernelTransactions kernelTransactions = newKernelTransactions();
        int expectedTransactions = 100;
        Phaser phaser = new Phaser(expectedTransactions);
        List<Future<Void>> transactionFutures = new ArrayList<>();
        for (int i = 0; i < expectedTransactions; i++) {
            Future transactionFuture = executorService.submit(() -> {
                try (KernelTransaction ignored =
                        kernelTransactions.newInstance(EXPLICIT, access(), EMBEDDED_CONNECTION, NO_TIMEOUT)) {
                    phaser.arriveAndAwaitAdvance();
                    assertEquals(expectedTransactions, kernelTransactions.getNumberOfActiveTransactions());
                    phaser.arriveAndAwaitAdvance();
                } catch (TransactionFailureException e) {
                    throw new RuntimeException(e);
                }
                phaser.arriveAndDeregister();
            });
            transactionFutures.add(transactionFuture);
        }
        combine(transactionFutures).get();
        assertEquals(0, kernelTransactions.getNumberOfActiveTransactions());
    }

    @Test
    void shouldRegisterTransactionMemoryPoolOnInit() throws Exception {
        // given
        GlobalMemoryGroupTracker memoryPools = new MemoryPools().pool(MemoryGroup.TRANSACTION, 0, null);
        KernelTransactions transactions = createTransactions(
                mock(StorageEngine.class, RETURNS_MOCKS),
                mock(TransactionCommitProcess.class),
                mock(TransactionIdStore.class),
                mock(KernelVersionProvider.class),
                DatabaseTracers.EMPTY,
                mock(LockManager.class),
                Clocks.nanoClock(),
                mock(AvailabilityGuard.class),
                Config.defaults(),
                memoryPools);
        assertThat(memoryPools.getDatabasePools()).isEmpty();

        // when
        transactions.init();

        // then
        assertThat(memoryPools.getDatabasePools().size()).isEqualTo(1);

        // and when
        transactions.shutdown();

        // then
        assertThat(memoryPools.getDatabasePools().size()).isEqualTo(0);
    }

    private static void stopKernelTransactions(KernelTransactions kernelTransactions) {
        try {
            kernelTransactions.stop();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private KernelTransactions newKernelTransactions() throws Throwable {
        return newKernelTransactions(mock(TransactionCommitProcess.class));
    }

    private KernelTransactions newKernelTransactions(Config config) throws Throwable {
        return newKernelTransactions(mock(TransactionCommitProcess.class), config);
    }

    private KernelTransactions newTestKernelTransactions() throws Throwable {
        return newKernelTransactions(
                true, mock(TransactionCommitProcess.class), mock(StorageReader.class), Config.defaults());
    }

    private KernelTransactions newKernelTransactions(TransactionCommitProcess commitProcess, Config config)
            throws Throwable {
        return newKernelTransactions(false, commitProcess, mock(StorageReader.class), config);
    }

    private KernelTransactions newKernelTransactions(TransactionCommitProcess commitProcess) throws Throwable {
        return newKernelTransactions(false, commitProcess, mock(StorageReader.class), Config.defaults());
    }

    private KernelTransactions newKernelTransactions(
            TransactionCommitProcess commitProcess, StorageReader firstReader, StorageReader... otherReaders)
            throws Throwable {
        return newKernelTransactions(false, commitProcess, firstReader, Config.defaults(), otherReaders);
    }

    private KernelTransactions newKernelTransactions(
            boolean testKernelTransactions,
            TransactionCommitProcess commitProcess,
            StorageReader firstReader,
            Config config,
            StorageReader... otherReaders)
            throws Throwable {
        LockManager.Client client = mock(LockManager.Client.class);
        when(locks.newClient()).thenReturn(client);

        StorageEngine storageEngine = mock(StorageEngine.class, RETURNS_MOCKS);
        when(storageEngine.newReader()).thenReturn(firstReader, otherReaders);
        when(storageEngine.newCommandCreationContext(anyBoolean())).thenReturn(mock(CommandCreationContext.class));
        when(storageEngine.createStorageCursors(any())).thenReturn(StoreCursors.NULL);
        when(storageEngine.createCommands(
                        any(ReadableTransactionState.class),
                        any(StorageReader.class),
                        any(CommandCreationContext.class),
                        any(LockTracer.class),
                        any(TxStateVisitor.Decorator.class),
                        any(CursorContext.class),
                        any(StoreCursors.class),
                        any(MemoryTracker.class)))
                .thenReturn(List.of(mock(StorageCommand.class)));

        return newKernelTransactions(locks, storageEngine, commitProcess, testKernelTransactions, config);
    }

    private KernelTransactions newKernelTransactions(
            LockManager locks,
            StorageEngine storageEngine,
            TransactionCommitProcess commitProcess,
            boolean testKernelTransactions,
            Config config) {
        LifeSupport life = new LifeSupport();
        life.start();

        TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
        when(transactionIdStore.getLastCommittedTransaction())
                .thenReturn(new TransactionId(0, 0, 0, UNKNOWN_CONSENSUS_INDEX));

        KernelVersionProvider kernelVersionProvider = LatestVersions.LATEST_KERNEL_VERSION_PROVIDER;

        DefaultTracers tracers = new DefaultTracers(
                "null", NullLog.getInstance(), new Monitors(), mock(JobScheduler.class), clock, config);
        final DatabaseTracers databaseTracers = new DatabaseTracers(tracers, DEFAULT_DATABASE_ID);

        KernelTransactions transactions;
        if (testKernelTransactions) {
            transactions = createTestTransactions(
                    storageEngine,
                    commitProcess,
                    transactionIdStore,
                    kernelVersionProvider,
                    databaseTracers,
                    locks,
                    clock,
                    databaseAvailabilityGuard);
        } else {
            transactions = createTransactions(
                    storageEngine,
                    commitProcess,
                    transactionIdStore,
                    kernelVersionProvider,
                    databaseTracers,
                    locks,
                    clock,
                    databaseAvailabilityGuard,
                    config,
                    new MemoryPools().pool(MemoryGroup.TRANSACTION, 0, null));
        }
        life.add(transactions);
        return transactions;
    }

    private static KernelTransactions createTransactions(
            StorageEngine storageEngine,
            TransactionCommitProcess commitProcess,
            TransactionIdStore transactionIdStore,
            KernelVersionProvider kernelVersionProvider,
            DatabaseTracers tracers,
            LockManager locks,
            SystemNanoClock clock,
            AvailabilityGuard databaseAvailabilityGuard,
            Config config,
            GlobalMemoryGroupTracker memoryGroupTracker) {
        return new KernelTransactions(
                config,
                locks,
                null,
                commitProcess,
                mock(DatabaseTransactionEventListeners.class),
                mock(TransactionMonitor.class),
                databaseAvailabilityGuard,
                storageEngine,
                mock(GlobalProcedures.class),
                mock(DbmsRuntimeVersionProvider.class),
                transactionIdStore,
                kernelVersionProvider,
                mock(ServerIdentity.class),
                clock,
                new AtomicReference<>(CpuClock.NOT_AVAILABLE),
                any -> CanWrite.INSTANCE,
                NULL_CONTEXT_FACTORY,
                ON_HEAP,
                mock(ConstraintSemantics.class),
                mock(SchemaState.class),
                mockedTokenHolders(),
                mock(ElementIdMapper.class),
                DEFAULT_DATABASE_ID,
                mock(IndexingService.class),
                mock(IndexStatisticsStore.class),
                createDependencies(),
                tracers,
                LeaseService.NO_LEASES,
                memoryGroupTracker,
                writable(),
                TransactionExecutionMonitor.NO_OP,
                snapshot -> true,
                mock(TransactionCommitmentFactory.class),
                new TransactionIdSequence(),
                TransactionIdGenerator.EMPTY,
                mock(DatabaseHealth.class),
                mock(LogicalTransactionStore.class),
                EMPTY_VALIDATOR_FACTORY,
                NullLogProvider.getInstance());
    }

    private static TestKernelTransactions createTestTransactions(
            StorageEngine storageEngine,
            TransactionCommitProcess commitProcess,
            TransactionIdStore transactionIdStore,
            KernelVersionProvider kernelVersionProvider,
            DatabaseTracers tracers,
            LockManager locks,
            SystemNanoClock clock,
            AvailabilityGuard databaseAvailabilityGuard) {
        Dependencies dependencies = createDependencies();
        return new TestKernelTransactions(
                locks,
                null,
                commitProcess,
                mock(DatabaseTransactionEventListeners.class),
                mock(TransactionMonitor.class),
                databaseAvailabilityGuard,
                tracers,
                storageEngine,
                mock(GlobalProcedures.class),
                transactionIdStore,
                kernelVersionProvider,
                clock,
                any -> CanWrite.INSTANCE,
                new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER),
                mockedTokenHolders(),
                dependencies);
    }

    private static Dependencies createDependencies() {
        final var enrichmentStrategy = mock(ApplyEnrichmentStrategy.class);
        when(enrichmentStrategy.check()).thenReturn(EnrichmentMode.OFF);

        return dependenciesOf(enrichmentStrategy, mock(GraphDatabaseFacade.class), CommunitySecurityLog.NULL_LOG);
    }

    private Future<?> unblockTxsInSeparateThread(final KernelTransactions kernelTransactions) {
        return executorService.submit(kernelTransactions::unblockNewTransactions);
    }

    private static void assertNotDone(Future<?> future) {
        assertFalse(future.isDone());
    }

    private static KernelTransactionHandle newHandle(KernelTransaction tx) {
        return new TestKernelTransactionHandle(tx);
    }

    private static KernelTransaction getKernelTransaction(KernelTransactions transactions) {
        return transactions.newInstance(IMPLICIT, AnonymousContext.access(), EMBEDDED_CONNECTION, NO_TIMEOUT);
    }

    private static TokenHolders mockedTokenHolders() {
        return new TokenHolders(mock(TokenHolder.class), mock(TokenHolder.class), mock(TokenHolder.class));
    }

    private static class TestKernelTransactions extends KernelTransactions {
        TestKernelTransactions(
                LockManager locks,
                ConstraintIndexCreator constraintIndexCreator,
                TransactionCommitProcess transactionCommitProcess,
                DatabaseTransactionEventListeners eventListeners,
                TransactionMonitor transactionMonitor,
                AvailabilityGuard databaseAvailabilityGuard,
                DatabaseTracers tracers,
                StorageEngine storageEngine,
                GlobalProcedures globalProcedures,
                TransactionIdStore transactionIdStore,
                KernelVersionProvider kernelVersionProvider,
                SystemNanoClock clock,
                AccessCapabilityFactory accessCapabilityFactory,
                CursorContextFactory contextFactory,
                TokenHolders tokenHolders,
                Dependencies databaseDependencies) {
            super(
                    Config.defaults(),
                    locks,
                    constraintIndexCreator,
                    transactionCommitProcess,
                    eventListeners,
                    transactionMonitor,
                    databaseAvailabilityGuard,
                    storageEngine,
                    globalProcedures,
                    mock(DbmsRuntimeVersionProvider.class),
                    transactionIdStore,
                    kernelVersionProvider,
                    mock(ServerIdentity.class),
                    clock,
                    new AtomicReference<>(CpuClock.NOT_AVAILABLE),
                    accessCapabilityFactory,
                    contextFactory,
                    ON_HEAP,
                    new StandardConstraintSemantics(),
                    mock(SchemaState.class),
                    tokenHolders,
                    mock(ElementIdMapper.class),
                    DEFAULT_DATABASE_ID,
                    mock(IndexingService.class),
                    mock(IndexStatisticsStore.class),
                    databaseDependencies,
                    tracers,
                    LeaseService.NO_LEASES,
                    new MemoryPools().pool(MemoryGroup.TRANSACTION, 0, null),
                    writable(),
                    TransactionExecutionMonitor.NO_OP,
                    snapshot -> true,
                    mock(TransactionCommitmentFactory.class),
                    new TransactionIdSequence(),
                    TransactionIdGenerator.EMPTY,
                    mock(DatabaseHealth.class),
                    mock(LogicalTransactionStore.class),
                    EMPTY_VALIDATOR_FACTORY,
                    NullLogProvider.getInstance());
        }

        @Override
        KernelTransactionHandle createHandle(KernelTransactionImplementation tx) {
            return new TestKernelTransactionHandle(tx);
        }
    }
}
