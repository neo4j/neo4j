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

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

import static org.neo4j.util.Preconditions.checkArgument;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.util.Preconditions.requireNonNegative;

/**
 * A {@link ReconciledTransactionTracker} used for standalone and clustered databases that have a reconciler and allow updates of the system database.
 * Updates can happen asynchronously and the task of this tracker is to keep track of all fully reconciled operations.
 */
public class DefaultReconciledTransactionTracker implements ReconciledTransactionTracker
{
    private static final int INITIAL_ARRAY_SIZE = 200;
    private static final long[] NO_METADATA = new long[0];

    /**
     * Lock protects {@link #getLastReconciledTransactionId()} and {@link #setLastReconciledTransactionId(long)}
     * from running concurrently with {@link #initialize(long)} and reading/updating a stale {@link #sequence}. This
     * is especially required when re-initialization happens at runtime and not during start of the lifecycle.
     * Re-initialization can happen after a store copy of the system database.
     */
    private final ReadWriteLock initializationLock;
    private final Log log;

    private long startingNumber;
    private OutOfOrderSequence sequence;

    public DefaultReconciledTransactionTracker( LogService logService )
    {
        initializationLock = new ReentrantReadWriteLock();
        log = logService.getInternalLog( getClass() );
    }

    @Override
    public void initialize( long reconciledTransactionId )
    {
        requireNonNegative( reconciledTransactionId );

        initializationLock.writeLock().lock();
        try
        {
            if ( sequence == null )
            {
                log.info( "Initializing with transaction ID %s", reconciledTransactionId );
            }
            else
            {
                log.info( "Re-initializing from %s to transaction ID %s", sequence, reconciledTransactionId );
            }
            sequence = new ArrayQueueOutOfOrderSequence( reconciledTransactionId, INITIAL_ARRAY_SIZE, NO_METADATA );
            startingNumber = reconciledTransactionId;
        }
        finally
        {
            initializationLock.writeLock().unlock();
        }
    }

    @Override
    public long getLastReconciledTransactionId()
    {
        initializationLock.readLock().lock();
        try
        {
            return sequence != null ? sequence.getHighestGapFreeNumber() : NO_RECONCILED_TRANSACTION_ID;
        }
        finally
        {
            initializationLock.readLock().unlock();
        }
    }

    @Override
    public void setLastReconciledTransactionId( long reconciledTransactionId )
    {
        requireNonNegative( reconciledTransactionId );

        initializationLock.readLock().lock();
        try
        {
            checkState( sequence != null, "Not initialized" );

            if ( reconciledTransactionId < startingNumber )
            {
                // this can happen when a store copy happens concurrently with a reconciliation
                log.info( "Ignoring pre-initialization ID  %s", reconciledTransactionId );
                return;
            }

            var currentLastReconciledTxId = getLastReconciledTransactionId();

            // gap-free ID should always be lower than the given ID
            checkArgument( reconciledTransactionId > currentLastReconciledTxId,
                    "Received illegal transaction ID %s which is lower than the current transaction ID %s. Sequence: %s",
                    reconciledTransactionId, currentLastReconciledTxId, sequence );

            log.debug( "Updating %s with transaction ID %s", sequence, reconciledTransactionId );
            sequence.offer( reconciledTransactionId, NO_METADATA );
        }
        finally
        {
            initializationLock.readLock().unlock();
        }
    }
}
