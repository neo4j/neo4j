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
package org.neo4j.kernel.database;

import static java.lang.Integer.max;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ExceptionUtils;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.DbmsRuntimeVersionProvider;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelImpl;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.kernel.internal.event.InternalTransactionEventListener;
import org.neo4j.lock.Lock;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.Race;

class DatabaseUpgradeTransactionHandlerTest {
    private volatile KernelVersion currentKernelVersion;
    private volatile DbmsRuntimeVersion currentDbmsRuntimeVersion;
    private InternalTransactionEventListener<Object> listener;
    private volatile boolean listenerUnregistered;
    private final ConcurrentLinkedQueue<RegisteredTransaction> registeredTransactions = new ConcurrentLinkedQueue<>();
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final RWUpgradeLocker lock = new RWUpgradeLocker();

    @AfterEach
    void checkTransactionStreamConsistency() {
        assertCorrectTransactionStream();
    }

    @Test
    void shouldUpdateKernelOnFirstTransactionAndUnsubscribeListener() throws TransactionFailureException {
        // Given
        init(KernelVersion.V4_2, LatestVersions.LATEST_RUNTIME_VERSION);

        // When
        doATransaction();

        // Then
        assertThat(currentKernelVersion).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);
        assertThat(listenerUnregistered).isTrue();

        LogAssertions.assertThat(logProvider)
                .containsMessageWithArguments(
                        "Upgrade transaction from %s to %s started",
                        KernelVersion.V4_2, LatestVersions.LATEST_KERNEL_VERSION)
                .containsMessageWithArguments(
                        "Upgrade transaction from %s to %s completed",
                        KernelVersion.V4_2, LatestVersions.LATEST_KERNEL_VERSION);
    }

    @Test
    void shouldNotRegisterListenerWhenOnLatestVersion() throws TransactionFailureException {
        // Given
        init(LatestVersions.LATEST_KERNEL_VERSION, LatestVersions.LATEST_RUNTIME_VERSION);

        // When
        doATransaction();

        // Then
        assertThat(listener).isNull();
        assertThat(listenerUnregistered).isFalse();
    }

    @Test
    void shouldNotUpgradePastRuntimeVersionAndKeepListener() throws TransactionFailureException {
        // Given
        init(KernelVersion.V4_2, DbmsRuntimeVersion.V4_3_D4);

        // When
        doATransaction();

        // Then
        assertThat(currentKernelVersion).isEqualTo(KernelVersion.V4_3_D4);
        assertThat(listenerUnregistered).isFalse();
    }

    @Test
    void shouldWaitForUpgradeUntilRuntimeVersionIsBumped() throws TransactionFailureException {
        // Given
        init(KernelVersion.V4_2, DbmsRuntimeVersion.V4_2);

        // When
        doATransaction();

        // Then
        assertThat(currentKernelVersion).isEqualTo(KernelVersion.V4_2);
        assertThat(listenerUnregistered).isFalse();

        // When
        setDbmsRuntime(LatestVersions.LATEST_RUNTIME_VERSION);
        doATransaction();

        // then
        assertThat(currentKernelVersion).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);
        assertThat(listenerUnregistered).isTrue();
    }

    @Test
    void shouldNotRegisterListenerWhenKernelIsNewerThanRuntime() throws TransactionFailureException {
        // Given
        init(LatestVersions.LATEST_KERNEL_VERSION, DbmsRuntimeVersion.V4_2);

        // When
        doATransaction();

        // Then
        assertThat(listener).isNull();
        assertThat(listenerUnregistered).isFalse();
    }

    @Test
    void shouldUpgradeOnceEvenWithManyConcurrentTransactions() throws TransactionFailureException {
        // Given a dbms running with 4.3 "jars" and a db that has just now been upgraded to run on these jars too
        init(KernelVersion.V4_2, DbmsRuntimeVersion.V4_4);
        AtomicBoolean stop = new AtomicBoolean();
        Race race = new Race().withEndCondition(stop::get);

        // When letting multiple transactions run, while in the midst of that upgrading the dbms runtime version
        race.addContestants(
                max(Runtime.getRuntime().availableProcessors() - 1, 2), this::doATransactionWithSomeSleeping);
        race.addContestant(
                () -> {
                    // Wait for the first upgrade transaction
                    // which really is the db setting its kernel version to that of the current dbms runtime version,
                    // which is 4.2
                    assertEventually(this::getKernelVersion, equalityCondition(KernelVersion.V4_4), 1, MINUTES);
                    // Then upgrade the dbms runtime version
                    setDbmsRuntime(LatestVersions.LATEST_RUNTIME_VERSION);
                    // And wait for the db to make the upgrade to this version too
                    assertEventually(
                            this::getKernelVersion,
                            equalityCondition(LatestVersions.LATEST_KERNEL_VERSION),
                            1,
                            MINUTES);
                    stop.set(true);
                },
                1);

        race.goUnchecked();
    }

    /**
     * Asserts that the observed transaction stream is consistent according to these rules:
     * - Start from some of the known versions
     * - All transactions up until the next upgrade transaction needs to be for the same version
     * - Upgrade transaction is expected to be last of the old version
     *
     * For example like this:
     * - V4_0,UPGRADE(V4_0->V4_2),V4_2,V4_2,UPGRADE(V4_2->V4_3),V4_3,V4_3,V4_3
     * - V4_2,V4_2,V4_2,UPGRADE(V4_2->V4_3),V4_3,V4_3
     */
    private void assertCorrectTransactionStream() {
        KernelVersion checkVersion = null;
        boolean justSawUpgrade = false;
        for (RegisteredTransaction registeredTransaction : registeredTransactions) {
            if (registeredTransaction.isUpgradeTransaction) {
                if (checkVersion != null) {
                    assertThat(registeredTransaction.version).isEqualTo(checkVersion);
                }
                checkVersion = registeredTransaction.version;
                justSawUpgrade = true;
            } else {
                if (checkVersion != null) {
                    if (justSawUpgrade) {
                        assertThat(registeredTransaction.version.isGreaterThan(checkVersion))
                                .isTrue();
                        checkVersion = registeredTransaction.version;
                    } else {
                        assertThat(registeredTransaction.version).isEqualTo(checkVersion);
                    }
                } else {
                    checkVersion = registeredTransaction.version;
                }
                justSawUpgrade = false;
            }
        }
    }

    private void init(KernelVersion initialKernelVersion, DbmsRuntimeVersion initialDbmsRuntimeVersion)
            throws TransactionFailureException {
        setKernelVersion(initialKernelVersion);
        setDbmsRuntime(initialDbmsRuntimeVersion);

        DbmsRuntimeVersionProvider dbmsRuntimeVersionProvider = mock(DbmsRuntimeVersionProvider.class);
        doAnswer(inv -> currentDbmsRuntimeVersion)
                .when(dbmsRuntimeVersionProvider)
                .getVersion();
        KernelVersionProvider kernelVersionProvider = this::getKernelVersion;
        DatabaseTransactionEventListeners databaseTransactionEventListeners =
                mock(DatabaseTransactionEventListeners.class);
        doAnswer(inv -> listener = inv.getArgument(0, InternalTransactionEventListener.class))
                .when(databaseTransactionEventListeners)
                .registerTransactionEventListener(any());
        doAnswer(inv -> listenerUnregistered = true)
                .when(databaseTransactionEventListeners)
                .unregisterTransactionEventListener(any());

        KernelImpl kernelMock = mock(KernelImpl.class);
        KernelTransaction txMock = mock(KernelTransaction.class);
        when(txMock.getTransactionSequenceNumber()).thenReturn(500L);
        when(kernelMock.beginTransaction(KernelTransaction.Type.IMPLICIT, AUTH_DISABLED))
                .thenReturn(txMock);
        DatabaseUpgradeTransactionHandler handler = new DatabaseUpgradeTransactionHandler(
                dbmsRuntimeVersionProvider,
                kernelVersionProvider,
                databaseTransactionEventListeners,
                lock,
                logProvider,
                Config.defaults(),
                kernelMock);
        handler.registerUpgradeListener((fromKernelVersion, toKernelVersion, tx) -> {
            // The tx being sent in here is just a mock, so we create the tx here
            // and treat it as a regular tx to see that we get pass beforeCommit for the upgrade tx.
            doATransaction(false, true, tx.getTransactionSequenceNumber());
            setKernelVersion(toKernelVersion);
        });
    }

    private synchronized void setKernelVersion(KernelVersion newKernelVersion) {
        assertThat(currentKernelVersion)
                .as("We only allow one upgrade transaction")
                .isNotEqualTo(newKernelVersion);
        currentKernelVersion = newKernelVersion;
    }

    private synchronized KernelVersion getKernelVersion() {
        return currentKernelVersion;
    }

    private synchronized void setDbmsRuntime(DbmsRuntimeVersion dbmsRuntimeVersion) {
        this.currentDbmsRuntimeVersion = dbmsRuntimeVersion;
    }

    private void doATransaction() {
        doATransaction(false, false, 100);
    }

    private void doATransactionWithSomeSleeping() {
        doATransaction(true, false, 100);
    }

    private void doATransaction(boolean doSomeSleeping, boolean isUpgrade, long txNbr) {
        if (!listenerUnregistered && listener != null) {
            try {
                KernelTransaction mock = mock(KernelTransaction.class);
                when(mock.getTransactionSequenceNumber()).thenReturn(txNbr);
                Object state =
                        listener.beforeCommit(mock(TransactionData.class), mock, mock(GraphDatabaseService.class));
                KernelVersion currentKernelVersion = this.currentKernelVersion;
                if (doSomeSleeping) {
                    // This sleep is an enabler for upgrade vs transactions race, makes this way more likely to trigger
                    Thread.sleep(ThreadLocalRandom.current().nextInt(3));
                }
                registeredTransactions.add(new RegisteredTransaction(currentKernelVersion, isUpgrade));
                // At this point we cannot assert on a comparison between dbms runtime version and kernel version
                listener.afterCommit(mock(TransactionData.class), state, mock(GraphDatabaseService.class));
            } catch (Exception e) {
                ExceptionUtils.throwAsUncheckedException(e);
            }
        }
    }

    private static class RegisteredTransaction {
        private final KernelVersion version;
        private final boolean isUpgradeTransaction;

        RegisteredTransaction(KernelVersion version, boolean isUpgradeTransaction) {
            this.version = version;
            this.isUpgradeTransaction = isUpgradeTransaction;
        }

        @Override
        public String toString() {
            return "RegisteredTransaction{" + "version=" + version + ", isUpgradeTransaction=" + isUpgradeTransaction
                    + '}';
        }
    }

    private static class RWUpgradeLocker implements UpgradeLocker {
        private final ReadWriteLock realLock = new ReentrantReadWriteLock();

        @Override
        public Lock acquireWriteLock(KernelTransaction tx) {
            realLock.writeLock().lock();
            return new Lock() {
                @Override
                public void release() {
                    realLock.writeLock().unlock();
                }
            };
        }

        @Override
        public Lock acquireReadLock(KernelTransaction tx) {
            realLock.readLock().lock();
            return new Lock() {
                @Override
                public void release() {
                    realLock.readLock().unlock();
                }
            };
        }
    }
}
