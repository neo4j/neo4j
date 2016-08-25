/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.txtracking;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.neo4j.function.Predicates.tryAwait;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

/**
 * Facility to allow a user to run a query on different members of the cluster and ensure that a member is at least as
 * up to date as the state it read or wrote elsewhere.
 */
public class TransactionIdTracker
{
    private static final int POLL_INTERVAL = 25;
    private static final TimeUnit POLL_UNIT = TimeUnit.MILLISECONDS;

    private final TransactionIdStore transactionIdStore;

    public TransactionIdTracker( TransactionIdStore transactionIdStore )
    {
        this.transactionIdStore = transactionIdStore;
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
     * @param oldestAcceptableTxId id of the Oldest Acceptable Transaction (OAT) that must have been applied before
     *                             continuing work.
     * @param timeout maximum duration to wait for OAT to be applied
     * @throws TransactionFailureException
     */
    public void awaitUpToDate( long oldestAcceptableTxId, Duration timeout ) throws TransactionFailureException
    {
        if ( oldestAcceptableTxId <= BASE_TX_ID )
        {
            return;
        }

        try
        {
            if ( !tryAwait( () -> oldestAcceptableTxId <= transactionIdStore.getLastClosedTransactionId(),
                    timeout.toMillis(), TimeUnit.MILLISECONDS, POLL_INTERVAL, POLL_UNIT ) )
            {
                throw new TransactionFailureException( Status.Transaction.InstanceStateChanged,
                        "Database not up to the requested version: %d. Latest database version is %d", oldestAcceptableTxId,
                        transactionIdStore.getLastClosedTransactionId() );
            }
        }
        catch ( InterruptedException e )
        {
            throw new TransactionFailureException( Status.Transaction.TransactionStartFailed, e,
                    "Thread interrupted when starting transaction" );
        }
    }

    /**
     * Find the id of the Newest Encountered Transaction (NET) that could have been seen on this server.
     * We expect the returned id to be sent back the client and ultimately supplied to
     * {@link #awaitUpToDate(long, Duration)} on this server, or on a different server in the cluster.
     *
     * @return id of the Newest Encountered Transaction (NET).
     */
    public long newestEncounteredTxId()
    {
        return transactionIdStore.getLastClosedTransactionId();
    }
}
