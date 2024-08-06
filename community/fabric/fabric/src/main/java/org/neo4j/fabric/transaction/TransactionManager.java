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
package org.neo4j.fabric.transaction;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.executor.FabricLocalExecutor;
import org.neo4j.fabric.executor.FabricRemoteExecutor;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.impl.api.transaction.trace.TraceProviderFactory;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.time.SystemNanoClock;

public class TransactionManager extends LifecycleAdapter {
    private final FabricRemoteExecutor remoteExecutor;
    private final FabricLocalExecutor localExecutor;
    private final Config config;
    private final ErrorReporter errorReporter;
    private final FabricTransactionMonitor transactionMonitor;
    private final AbstractSecurityLog securityLog;
    private final SystemNanoClock clock;

    private final Set<FabricTransactionImpl> openTransactions = ConcurrentHashMap.newKeySet();
    private final long awaitActiveTransactionDeadlineMillis;
    private final AvailabilityGuard availabilityGuard;
    private final GlobalProcedures globalProcedures;
    private final CatalogManager catalogManager;

    public TransactionManager(
            FabricRemoteExecutor remoteExecutor,
            FabricLocalExecutor localExecutor,
            CatalogManager catalogManager,
            FabricTransactionMonitor transactionMonitor,
            AbstractSecurityLog securityLog,
            SystemNanoClock clock,
            Config config,
            AvailabilityGuard availabilityGuard,
            ErrorReporter errorReporter,
            GlobalProcedures globalProcedures) {
        this.remoteExecutor = remoteExecutor;
        this.localExecutor = localExecutor;
        this.catalogManager = catalogManager;
        this.config = config;
        this.errorReporter = errorReporter;
        this.transactionMonitor = transactionMonitor;
        this.securityLog = securityLog;
        this.clock = clock;
        this.awaitActiveTransactionDeadlineMillis = config.get(GraphDatabaseSettings.shutdown_transaction_end_timeout)
                .toMillis();
        this.availabilityGuard = availabilityGuard;
        this.globalProcedures = globalProcedures;
    }

    public FabricTransaction begin(
            FabricTransactionInfo transactionInfo, TransactionBookmarkManager transactionBookmarkManager) {
        if (availabilityGuard.isShutdown()) {
            throw new DatabaseShutdownException();
        }

        var sessionDb = transactionInfo.getSessionDatabaseReference();

        transactionInfo.getLoginContext().authorize(LoginContext.IdLookup.EMPTY, sessionDb, securityLog);

        var procedures = new FabricProcedures(globalProcedures.getCurrentView());

        FabricTransactionImpl fabricTransaction = new FabricTransactionImpl(
                transactionInfo,
                transactionBookmarkManager,
                remoteExecutor,
                localExecutor,
                procedures,
                errorReporter,
                this,
                catalogManager.currentCatalog(),
                catalogManager,
                sessionDb instanceof DatabaseReferenceImpl.Composite,
                clock,
                TraceProviderFactory.getTraceProvider(config));

        openTransactions.add(fabricTransaction);
        transactionMonitor.startMonitoringTransaction(fabricTransaction, transactionInfo);
        return fabricTransaction;
    }

    @Override
    public void stop() {
        // On a fabric level we will deal with transactions that a cross DBMS.
        // Any db specific transaction will be handled on a database level with own set of rules, checks etc
        var nonLocalTransaction = collectNonLocalTransactions();
        if (nonLocalTransaction.isEmpty()) {
            return;
        }
        awaitTransactionsClosedWithinTimeout(nonLocalTransaction);
        nonLocalTransaction.forEach(tx -> tx.markForTermination(Status.Transaction.Terminated));
    }

    private Collection<FabricTransactionImpl> collectNonLocalTransactions() {
        return openTransactions.stream().filter(tx -> !tx.isLocal()).collect(Collectors.toList());
    }

    private void awaitTransactionsClosedWithinTimeout(Collection<FabricTransactionImpl> nonLocalTransaction) {
        long deadline = clock.millis() + awaitActiveTransactionDeadlineMillis;
        while (hasOpenTransactions(nonLocalTransaction) && clock.millis() < deadline) {
            parkNanos(MILLISECONDS.toNanos(10));
        }
    }

    private static boolean hasOpenTransactions(Collection<FabricTransactionImpl> nonLocalTransaction) {
        for (FabricTransactionImpl fabricTransaction : nonLocalTransaction) {
            if (fabricTransaction.isOpen()) {
                return true;
            }
        }
        return false;
    }

    void removeTransaction(FabricTransactionImpl transaction) {
        openTransactions.remove(transaction);
        transactionMonitor.stopMonitoringTransaction(transaction);
    }

    public Set<FabricTransaction> getOpenTransactions() {
        return Collections.unmodifiableSet(openTransactions);
    }

    public Optional<FabricTransaction> findTransactionContaining(InternalTransaction transaction) {
        return openTransactions.stream()
                .filter(tx -> tx.getInternalTransactions().stream()
                        .anyMatch(itx -> itx.kernelTransaction() == transaction.kernelTransaction()))
                .map(FabricTransaction.class::cast)
                .findFirst();
    }
}
