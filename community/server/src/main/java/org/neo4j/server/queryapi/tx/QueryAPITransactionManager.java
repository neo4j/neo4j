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
package org.neo4j.server.queryapi.tx;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.scheduler.JobMonitoringParams.systemJob;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.internal.InternalSession;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.queryapi.metrics.QueryAPIMetricsMonitor;

public class QueryAPITransactionManager implements TransactionManager {

    private final Map<String, Transaction> transactions = new ConcurrentHashMap<>();
    private final Duration timeout;
    private final QueryAPIMetricsMonitor monitor;

    public QueryAPITransactionManager(Duration timeout, JobScheduler scheduler, QueryAPIMetricsMonitor monitor) {
        this.timeout = timeout;
        this.monitor = monitor;
        scheduler.scheduleRecurring(
                Group.SERVER_TRANSACTION_TIMEOUT,
                systemJob("Timeout of Query API transactions"),
                this::beginTimeoutJob,
                5,
                SECONDS);
    }

    @Override
    public Transaction begin(
            String txId,
            Session session,
            AuthToken authToken,
            String databaseName,
            TransactionConfig config,
            String txType)
            throws TransactionIdCollisionException {
        monitor.openTransaction();
        var internalSession = (InternalSession) session; // needed to support txType
        var driverTransaction = internalSession.beginTransaction(config, txType);
        var tx = new QueryAPITransaction(
                txId,
                driverTransaction,
                session,
                authToken,
                databaseName,
                Instant.now().plus(timeout).truncatedTo(ChronoUnit.SECONDS),
                timeout);
        var existingTx = transactions.putIfAbsent(txId, tx);

        if (existingTx != null) {
            throw new TransactionIdCollisionException();
        }

        tx.tryAcquire();
        return tx;
    }

    @Override
    public Transaction retrieveTransaction(String transactionId, String requestedDatabase, AuthToken accessingUser)
            throws TransactionNotFoundException {
        var tx = transactions.get(transactionId);

        if (tx != null) {
            if (tx.tryAcquire()) {
                if (tx.databaseName().equals(requestedDatabase)
                        && tx.authToken().equals(accessingUser)) {
                    return tx;
                } else {
                    tx.release();
                }
            } else {
                throw new TransactionConcurrentAccessException("Transaction was accessed concurrently");
            }
        }
        throw new TransactionNotFoundException(transactionId);
    }

    @Override
    public void releaseTransaction(String txId) {
        var tx = transactions.get(txId);

        if (tx != null) {
            tx.release();
        }
    }

    @Override
    public void removeTransaction(String txId) {
        var tx = transactions.get(txId);

        if (tx != null) {
            transactions.remove(txId);
            tx.close();
            monitor.closeTransaction();
            tx.release();
        }
    }

    @Override
    public void beginTimeoutJob() {
        var timeoutFrom = Instant.now();

        for (Map.Entry<String, Transaction> tx : transactions.entrySet()) {
            if (tx.getValue().tryAcquire()) {
                if (timeoutFrom.compareTo(tx.getValue().expiresAt()) > 0) {
                    removeTransaction(tx.getKey());
                    monitor.totalTransactionsTimedOut();
                }
                tx.getValue().release();
            }
        }
    }

    @Override
    public long openTransactionCount() {
        return transactions.size();
    }
}
