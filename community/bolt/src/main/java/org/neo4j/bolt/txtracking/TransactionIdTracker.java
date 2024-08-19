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
package org.neo4j.bolt.txtracking;

import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.General.DatabaseUnavailable;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.BookmarkTimeout;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlMessageParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.database.AbstractDatabase;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.Stopwatch;
import org.neo4j.time.SystemNanoClock;

/**
 * Facility to allow a user to run a query on different members of the cluster and ensure that a member is at least as
 * up to date as the state it read or wrote elsewhere.
 */
public class TransactionIdTracker {
    private final DatabaseManagementService managementService;
    private final TransactionIdTrackerMonitor monitor;
    private final SystemNanoClock clock;

    private final Log log;

    public TransactionIdTracker(
            DatabaseManagementService managementService,
            Monitors monitors,
            SystemNanoClock clock,
            LogProvider logProvider) {
        this.managementService = managementService;
        this.monitor = monitors.newMonitor(TransactionIdTrackerMonitor.class);
        this.clock = clock;
        this.log = logProvider.getLog(getClass());
    }

    /**
     * Find the id of the Newest Encountered Transaction (NET) that could have been seen on this server for the specified database.
     * We expect the returned id to be sent back the client and ultimately supplied to
     * {@link #awaitUpToDate(NamedDatabaseId, long, Duration)} on this server, or on a different server in the cluster.
     *
     * @param namedDatabaseId id of the database to find the NET for.
     * @return id of the Newest Encountered Transaction (NET).
     */
    public long newestTransactionId(NamedDatabaseId namedDatabaseId) {
        var db = database(namedDatabaseId);
        try {
            // return the "last committed" because it is the newest id
            // "last closed" will return the last gap-free id, potentially for some old transaction because there might
            // be other committing transactions
            // "last reconciled" might also return an id lower than the ID of the just committed transaction
            return transactionIdStore(db).getLastCommittedTransactionId();
        } catch (RuntimeException e) {
            throw databaseUnavailable(db, e);
        }
    }

    /**
     * Wait for a specific transaction (the Oldest Acceptable Transaction - OAT) to have been applied before
     * continuing. This method is useful in a clustered deployment, where different members of the cluster are expected
     * to apply transactions at slightly different times.
     * <p>
     * We assume the OAT will always have been applied on one member of the cluster, therefore it is sensible to wait
     * for it to be applied on this member.
     * <p>
     * The effect is either:
     * <ol>
     *     <li>If the transaction in question has already been applied, return immediately.
     *     This is the most common case because we expect the interval between dependent requests from the client
     *     to be longer than the replication lag between cluster members.</li>
     *     <li>The transaction has not yet been applied, block until the background replication process has applied it,
     *     or timeout.</li>
     * </ol>
     *
     * @param namedDatabaseId id of the database to find the transaction id.
     * @param oldestAcceptableTxId id of the Oldest Acceptable Transaction (OAT) that must have been applied before
     * continuing work.
     * @param timeout maximum duration to wait for OAT to be applied
     */
    public void awaitUpToDate(NamedDatabaseId namedDatabaseId, long oldestAcceptableTxId, Duration timeout) {
        var db = database(namedDatabaseId);

        if (oldestAcceptableTxId <= BASE_TX_ID) {
            return;
        }

        var lastTransactionId = -1L;
        try {
            Stopwatch startTime = clock.startStopWatch();
            boolean waited = false;
            do {
                if (isNotAvailable(db)) {
                    throw databaseUnavailable(db);
                }
                lastTransactionId = currentTransactionId(db);
                if (oldestAcceptableTxId <= lastTransactionId) {
                    log.debug(
                            "Done waiting for bookmark on database '%s' after %s (awaited:%s, reached:%s)",
                            namedDatabaseId,
                            waited ? startTime.elapsed() : Duration.ZERO,
                            oldestAcceptableTxId,
                            lastTransactionId);
                    return;
                }
                waitWhenNotUpToDate();
                waited = true;
            } while (!startTime.hasTimedOut(timeout));

            throw unreachableDatabaseVersion(db, lastTransactionId, oldestAcceptableTxId);
        } catch (RuntimeException e) {
            if (isNotAvailable(db)) {
                throw databaseUnavailable(db, e);
            }
            throw unreachableDatabaseVersion(db, lastTransactionId, oldestAcceptableTxId, e);
        }
    }

    private void waitWhenNotUpToDate() {
        monitor.onWaitWhenNotUpToDate();
        LockSupport.parkNanos(100);
    }

    private static long currentTransactionId(AbstractDatabase db) {
        // await for the last closed transaction id to to have at least the expected value
        // it has to be "last closed" and not "last committed" because all transactions before the expected one should
        // also be committed
        return transactionIdStore(db).getLastClosedTransactionId();
    }

    private static TransactionIdStore transactionIdStore(AbstractDatabase db) {
        // We need to resolve this as late as possible in case the database has been restarted as part of store copy.
        // This causes TransactionIdStore staleness and we could get a MetaDataStore closed exception.
        // Ideally we'd fix this with some life cycle wizardry but not going to do that for now.
        return db.getDependencyResolver().resolveDependency(TransactionIdStore.class);
    }

    private AbstractDatabase database(NamedDatabaseId namedDatabaseId) {
        try {
            var dbApi = (GraphDatabaseAPI) managementService.database(namedDatabaseId.name());
            var db = dbApi.getDependencyResolver().resolveDependency(AbstractDatabase.class);
            if (isNotAvailable(db)) {
                throw databaseUnavailable(db);
            }
            return db;
        } catch (DatabaseNotFoundException e) {
            throw databaseNotFound(namedDatabaseId);
        }
    }

    private static boolean isNotAvailable(AbstractDatabase db) {
        return !db.getDatabaseAvailabilityGuard().isAvailable();
    }

    private static TransactionIdTrackerException databaseNotFound(NamedDatabaseId namedDatabaseId) {
        return new TransactionIdTrackerException(
                DatabaseNotFound, "Database '" + namedDatabaseId.name() + "' does not exist");
    }

    private static TransactionIdTrackerException databaseUnavailable(AbstractDatabase db) {
        return databaseUnavailable(db, null);
    }

    private static TransactionIdTrackerException databaseUnavailable(AbstractDatabase db, Throwable cause) {
        return new TransactionIdTrackerException(
                DatabaseUnavailable, "Database '" + db.getNamedDatabaseId().name() + "' unavailable", cause);
    }

    private static TransactionIdTrackerException unreachableDatabaseVersion(
            AbstractDatabase db, long lastTransactionId, long oldestAcceptableTxId) {
        return unreachableDatabaseVersion(db, lastTransactionId, oldestAcceptableTxId, null);
    }

    private static TransactionIdTrackerException unreachableDatabaseVersion(
            AbstractDatabase db, long lastTransactionId, long oldestAcceptableTxId, Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N13)
                .withClassification(ErrorClassification.TRANSIENT_ERROR)
                .withParam(GqlMessageParams.dbName, db.getNamedDatabaseId().name())
                .withParam(GqlMessageParams.oldestAcceptableTxId, String.valueOf(oldestAcceptableTxId))
                .withParam(GqlMessageParams.latestTransactionId, String.valueOf(lastTransactionId))
                .build();
        return new TransactionIdTrackerException(
                gql,
                BookmarkTimeout,
                "Database '" + db.getNamedDatabaseId().name() + "' not up to the requested version: "
                        + oldestAcceptableTxId + ". " + "Latest database version is " + lastTransactionId,
                cause);
    }
}
