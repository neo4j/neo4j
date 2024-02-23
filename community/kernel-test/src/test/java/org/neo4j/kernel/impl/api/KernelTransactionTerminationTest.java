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

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.api.TransactionTimeout.NO_TIMEOUT;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.kernel.impl.api.transaction.serial.DatabaseSerialGuard.EMPTY_GUARD;
import static org.neo4j.kernel.impl.locking.NoLocksClient.NO_LOCKS_CLIENT;
import static org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier.ON_HEAP;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.collection.Dependencies;
import org.neo4j.collection.pool.Pool;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.ProcedureView;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.monitoring.TransactionMonitor;
import org.neo4j.kernel.impl.query.TransactionExecutionMonitor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.resources.CpuClock;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.enrichment.ApplyEnrichmentStrategy;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidatorFactory;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.Race;
import org.neo4j.time.Clocks;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.ElementIdMapper;

class KernelTransactionTerminationTest {
    private static final int TEST_RUN_TIME_SECS = 5;

    @Test
    @Timeout(TEST_RUN_TIME_SECS * 20)
    void transactionCantBeTerminatedAfterItIsClosed() throws Throwable {
        runTwoThreads(() -> {}, tx -> tx.markForTermination(Status.Transaction.TransactionMarkedAsFailed), tx -> {
            close(tx);
            assertFalse(tx.getReasonIfTerminated().isPresent());
            tx.initialize();
        });
    }

    @Test
    @Timeout(TEST_RUN_TIME_SECS * 20)
    void closeTransaction() throws Throwable {
        BlockingQueue<Boolean> committerToTerminator = new LinkedBlockingQueue<>(1);
        BlockingQueue<TerminatorAction> terminatorToCommitter = new LinkedBlockingQueue<>(1);
        AtomicBoolean t1Done = new AtomicBoolean();

        runTwoThreads(
                () -> {
                    committerToTerminator.clear();
                    terminatorToCommitter.clear();
                    t1Done.set(false);
                },
                tx -> {
                    Boolean terminatorShouldAct = committerToTerminator.poll();
                    if (terminatorShouldAct != null && terminatorShouldAct) {
                        TerminatorAction action = TerminatorAction.random();
                        action.executeOn(tx);
                        assertTrue(terminatorToCommitter.add(action));
                    }
                    t1Done.set(true);
                },
                tx -> {
                    CommitterAction committerAction = CommitterAction.random();
                    if (committerToTerminator.offer(true)) {
                        TerminatorAction terminatorAction = null;
                        try {
                            // This loop optimizes the wait instead of waiting potentially a long time for T1 when it
                            // would lose the race and not do anything
                            while (!t1Done.get() && terminatorAction == null) {
                                terminatorAction = terminatorToCommitter.poll(10, MILLISECONDS);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (terminatorAction != null) {
                            close(tx, committerAction, terminatorAction);
                        }
                    }
                });
    }

    private static void runTwoThreads(
            Runnable cleaner,
            Consumer<TestKernelTransaction> thread1Action,
            Consumer<TestKernelTransaction> thread2Action)
            throws Throwable {
        try (TestKernelTransaction tx = TestKernelTransaction.create()) {
            long endTime = currentTimeMillis() + SECONDS.toMillis(TEST_RUN_TIME_SECS);
            int limit = 20_000;
            for (int i = 0; i < limit && currentTimeMillis() < endTime; i++) {
                cleaner.run();
                tx.initialize();
                Race race = new Race().withRandomStartDelays(0, 10);
                race.withEndCondition(() -> currentTimeMillis() >= endTime);
                race.addContestant(() -> thread1Action.accept(tx), 1);
                race.addContestant(() -> thread2Action.accept(tx), 1);
                race.go();
            }
        }
    }

    private static void close(KernelTransaction tx) {
        try {
            tx.close();
        } catch (TransactionFailureException e) {
            throw new RuntimeException(e);
        }
    }

    private static void close(TestKernelTransaction tx, CommitterAction committer, TerminatorAction terminator) {
        try {
            if (terminator == TerminatorAction.NONE) {
                committer.closeNotTerminated(tx);
            } else {
                committer.closeTerminated(tx);
            }
        } catch (TransactionFailureException e) {
            throw new RuntimeException(e);
        }
    }

    private enum TerminatorAction {
        NONE {
            @Override
            void executeOn(KernelTransaction tx) {}
        },
        TERMINATE {
            @Override
            void executeOn(KernelTransaction tx) {
                tx.markForTermination(Status.Transaction.TransactionMarkedAsFailed);
            }
        };

        abstract void executeOn(KernelTransaction tx);

        static TerminatorAction random() {
            return ThreadLocalRandom.current().nextBoolean() ? TERMINATE : NONE;
        }
    }

    private enum CommitterAction {
        NONE {
            @Override
            void closeTerminated(TestKernelTransaction tx) throws TransactionFailureException {
                tx.assertTerminated();
                tx.close();
                tx.assertRolledBack();
            }

            @Override
            void closeNotTerminated(TestKernelTransaction tx) throws TransactionFailureException {
                tx.assertNotTerminated();
                tx.close();
                tx.assertRolledBack();
            }
        },
        MARK_SUCCESS {
            @Override
            void closeTerminated(TestKernelTransaction tx) {
                tx.assertTerminated();
                assertThrows(TransactionTerminatedException.class, tx::commit);
                tx.assertRolledBack();
            }

            @Override
            void closeNotTerminated(TestKernelTransaction tx) throws TransactionFailureException {
                tx.assertNotTerminated();
                tx.commit();
                tx.assertCommitted();
            }
        },
        MARK_FAILURE {
            @Override
            void closeTerminated(TestKernelTransaction tx) throws TransactionFailureException {
                NONE.closeTerminated(tx);
            }

            @Override
            void closeNotTerminated(TestKernelTransaction tx) throws TransactionFailureException {
                NONE.closeNotTerminated(tx);
            }
        };

        static final CommitterAction[] VALUES = values();

        abstract void closeTerminated(TestKernelTransaction tx) throws TransactionFailureException;

        abstract void closeNotTerminated(TestKernelTransaction tx) throws TransactionFailureException;

        static CommitterAction random() {
            return VALUES[ThreadLocalRandom.current().nextInt(VALUES.length)];
        }
    }

    private static class TestKernelTransaction extends KernelTransactionImplementation {
        final CommitTrackingMonitor monitor;

        TestKernelTransaction(CommitTrackingMonitor monitor, Dependencies dependencies) {
            super(
                    Config.defaults(),
                    mock(DatabaseTransactionEventListeners.class),
                    mock(ConstraintIndexCreator.class),
                    mock(TransactionCommitProcess.class),
                    monitor,
                    mock(Pool.class),
                    Clocks.fakeClock(),
                    new AtomicReference<>(CpuClock.NOT_AVAILABLE),
                    mock(DatabaseTracers.class, RETURNS_MOCKS),
                    mock(StorageEngine.class, RETURNS_MOCKS),
                    any -> CanWrite.INSTANCE,
                    new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER),
                    ON_HEAP,
                    new StandardConstraintSemantics(),
                    mock(SchemaState.class),
                    mockedTokenHolders(),
                    mock(ElementIdMapper.class),
                    mock(IndexingService.class),
                    mock(IndexStatisticsStore.class),
                    dependencies,
                    from(DEFAULT_DATABASE_NAME, UUID.randomUUID()),
                    LeaseService.NO_LEASES,
                    MemoryPools.NO_TRACKING,
                    DatabaseReadOnlyChecker.writable(),
                    TransactionExecutionMonitor.NO_OP,
                    CommunitySecurityLog.NULL_LOG,
                    mockLocks(),
                    mock(TransactionCommitmentFactory.class),
                    mock(KernelTransactions.class),
                    TransactionIdGenerator.EMPTY,
                    mock(DbmsRuntimeRepository.class),
                    LatestVersions.LATEST_KERNEL_VERSION_PROVIDER,
                    mock(LogicalTransactionStore.class),
                    mock(ServerIdentity.class),
                    ApplyEnrichmentStrategy.NO_ENRICHMENT,
                    mock(DatabaseHealth.class),
                    NullLogProvider.getInstance(),
                    TransactionValidatorFactory.EMPTY_VALIDATOR_FACTORY,
                    EMPTY_GUARD,
                    false);

            this.monitor = monitor;
        }

        private static LockManager mockLocks() {
            var locks = mock(LockManager.class);
            when(locks.newClient()).thenReturn(NO_LOCKS_CLIENT);
            return locks;
        }

        static TestKernelTransaction create() {
            return new TestKernelTransaction(
                    new CommitTrackingMonitor(), dependenciesOf(mock(GraphDatabaseFacade.class)));
        }

        TestKernelTransaction initialize() {
            initialize(
                    42, Type.IMPLICIT, AUTH_DISABLED, NO_TIMEOUT, 1L, EMBEDDED_CONNECTION, mock(ProcedureView.class));
            monitor.reset();
            return this;
        }

        void assertCommitted() {
            assertTrue(monitor.committed);
        }

        void assertRolledBack() {
            assertTrue(monitor.rolledBack);
        }

        void assertTerminated() {
            assertEquals(
                    Status.Transaction.TransactionMarkedAsFailed,
                    getReasonIfTerminated().orElseThrow());
            assertTrue(monitor.terminated);
        }

        void assertNotTerminated() {
            assertFalse(getReasonIfTerminated().isPresent());
            assertFalse(monitor.terminated);
        }

        private static TokenHolders mockedTokenHolders() {
            return new TokenHolders(mock(TokenHolder.class), mock(TokenHolder.class), mock(TokenHolder.class));
        }
    }

    private static class CommitTrackingMonitor implements TransactionMonitor {
        volatile boolean committed;
        volatile boolean rolledBack;
        volatile boolean terminated;

        @Override
        public void transactionStarted() {}

        @Override
        public void transactionFinished(boolean successful, boolean writeTx) {
            if (successful) {
                committed = true;
            } else {
                rolledBack = true;
            }
        }

        @Override
        public void transactionTerminated(boolean writeTx) {
            terminated = true;
        }

        @Override
        public void upgradeToWriteTransaction() {}

        @Override
        public void transactionValidationFailure(DatabaseFile databaseFile) {}

        @Override
        public void addHeapTransactionSize(long transactionSizeHeap) {}

        @Override
        public void addNativeTransactionSize(long transactionSizeNative) {}

        void reset() {
            committed = false;
            rolledBack = false;
            terminated = false;
        }
    }
}
