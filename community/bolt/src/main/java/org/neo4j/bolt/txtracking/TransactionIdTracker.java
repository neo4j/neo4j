/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.txtracking;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.General.DatabaseUnavailable;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.BookmarkTimeout;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

/**
 * Facility to allow a user to run a query on different members of the cluster and ensure that a member is at least as
 * up to date as the state it read or wrote elsewhere.
 */
public class TransactionIdTracker
{
    private final DatabaseManagementService managementService;
    private final ReconciledTransactionTracker reconciledTxTracker;
    private final TransactionIdTrackerMonitor monitor;
    private final SystemNanoClock clock;

    public TransactionIdTracker( DatabaseManagementService managementService, ReconciledTransactionTracker reconciledTxTracker,
            Monitors monitors, SystemNanoClock clock )
    {
        this.managementService = managementService;
        this.reconciledTxTracker = reconciledTxTracker;
        this.monitor = monitors.newMonitor( TransactionIdTrackerMonitor.class );
        this.clock = clock;
    }

    /**
     * Find the id of the Newest Encountered Transaction (NET) that could have been seen on this server for the specified database.
     * We expect the returned id to be sent back the client and ultimately supplied to
     * {@link #awaitUpToDate(DatabaseId, long, Duration)} on this server, or on a different server in the cluster.
     *
     * @param databaseId id of the database to find the NET for.
     * @return id of the Newest Encountered Transaction (NET).
     */
    public long newestTransactionId( DatabaseId databaseId )
    {
        var db = database( databaseId );
        try
        {
            // return the "last committed" because it is the newest id
            // "last closed" will return the last gap-free id, potentially for some old transaction because there might be other committing transactions
            // "last reconciled" might also return an id lower than the ID of the just committed transaction
            return transactionIdStore( db ).getLastCommittedTransactionId();
        }
        catch ( RuntimeException e )
        {
            throw databaseUnavailable( db, e );
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
     * @param databaseId id of the database to find the transaction id.
     * @param oldestAcceptableTxId id of the Oldest Acceptable Transaction (OAT) that must have been applied before
     * continuing work.
     * @param timeout maximum duration to wait for OAT to be applied
     */
    public void awaitUpToDate( DatabaseId databaseId, long oldestAcceptableTxId, Duration timeout )
    {
        var db = database( databaseId );

        if ( oldestAcceptableTxId <= BASE_TX_ID )
        {
            return;
        }

        var lastTransactionId = -1L;
        try
        {
            var endTime = Math.addExact( clock.nanos(), timeout.toNanos() );
            while ( endTime > clock.nanos() )
            {
                if ( isNotAvailable( db ) )
                {
                    throw databaseUnavailable( db );
                }
                lastTransactionId = currentTransactionId( db );
                if ( oldestAcceptableTxId <= lastTransactionId )
                {
                    return;
                }
                waitWhenNotUpToDate();
            }
            throw unreachableDatabaseVersion( db, lastTransactionId, oldestAcceptableTxId );
        }
        catch ( RuntimeException e )
        {
            if ( isNotAvailable( db ) )
            {
                throw databaseUnavailable( db, e );
            }
            throw unreachableDatabaseVersion( db, lastTransactionId, oldestAcceptableTxId, e );
        }
    }

    private void waitWhenNotUpToDate()
    {
        monitor.onWaitWhenNotUpToDate();
        LockSupport.parkNanos( 100 );
    }

    private long currentTransactionId( Database db )
    {
        if ( db.isSystem() )
        {
            return reconciledTxTracker.getLastReconciledTransactionId();
        }
        else
        {
            // await for the last closed transaction id to to have at least the expected value
            // it has to be "last closed" and not "last committed" because all transactions before the expected one should also be committed
            return transactionIdStore( db ).getLastClosedTransactionId();
        }
    }

    private static TransactionIdStore transactionIdStore( Database db )
    {
        // We need to resolve this as late as possible in case the database has been restarted as part of store copy.
        // This causes TransactionIdStore staleness and we could get a MetaDataStore closed exception.
        // Ideally we'd fix this with some life cycle wizardry but not going to do that for now.
        return db.getDependencyResolver().resolveDependency( TransactionIdStore.class );
    }

    private Database database( DatabaseId databaseId )
    {
        try
        {
            var dbApi = (GraphDatabaseAPI) managementService.database( databaseId.name() );
            var db = dbApi.getDependencyResolver().resolveDependency( Database.class );
            if ( isNotAvailable( db ) )
            {
                throw databaseUnavailable( db );
            }
            return db;
        }
        catch ( DatabaseNotFoundException e )
        {
            throw databaseNotFound( databaseId );
        }
    }

    private static boolean isNotAvailable( Database db )
    {
        return !db.getDatabaseAvailabilityGuard().isAvailable();
    }

    private static TransactionIdTrackerException databaseNotFound( DatabaseId databaseId )
    {
        return new TransactionIdTrackerException( DatabaseNotFound, "Database '" + databaseId.name() + "' does not exist" );
    }

    private static TransactionIdTrackerException databaseUnavailable( Database db )
    {
        return databaseUnavailable( db, null );
    }

    private static TransactionIdTrackerException databaseUnavailable( Database db, Throwable cause )
    {
        return new TransactionIdTrackerException( DatabaseUnavailable, "Database '" + db.getDatabaseId().name() + "' unavailable", cause );
    }

    private static TransactionIdTrackerException unreachableDatabaseVersion( Database db, long lastTransactionId, long oldestAcceptableTxId )
    {
        return unreachableDatabaseVersion( db, lastTransactionId, oldestAcceptableTxId, null );
    }

    private static TransactionIdTrackerException unreachableDatabaseVersion( Database db, long lastTransactionId, long oldestAcceptableTxId, Throwable cause )
    {
        return new TransactionIdTrackerException( BookmarkTimeout,
                "Database '" + db.getDatabaseId().name() + "' not up to the requested version: " + oldestAcceptableTxId + ". " +
                "Latest database version is " + lastTransactionId, cause );
    }
}
