/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Can accept offerings about {@link TransactionId}, but will always only keep the highest one,
 * always available in {@link #get()}.
 */
public class HighestTransactionId
{
    private final AtomicReference<TransactionId> highest = new AtomicReference<>();

    public HighestTransactionId( long initialTransactionId, long initialChecksum, long commitTimestamp )
    {
        set( initialTransactionId, initialChecksum, commitTimestamp );
    }

    /**
     * Offers a transaction id. Will be accepted if this is higher than the current highest.
     * This method is thread-safe.
     *
     * @param transactionId transaction id to compare for highest.
     * @param checksum checksum of the transaction.
     * @param commitTimestamp commit time for transaction with {@code transactionId}.
     * @return {@code true} if the given transaction id was higher than the current highest,
     * {@code false}.
     */
    public boolean offer( long transactionId, long checksum, long commitTimestamp )
    {
        TransactionId high = highest.get();
        if ( transactionId < high.transactionId() )
        {   // a higher id has already been offered
            return false;
        }

        TransactionId update = new TransactionId( transactionId, checksum, commitTimestamp );
        while ( !highest.compareAndSet( high, update ) )
        {
            high = highest.get();
            if ( high.transactionId() >= transactionId )
            {   // apparently someone else set a higher id while we were trying to set this id
                return false;
            }
        }
        // we set our id as the highest
        return true;
    }

    /**
     * Overrides the highest transaction id value, no matter what it currently is. Used for initialization purposes.
     *
     * @param transactionId id of the transaction.
     * @param checksum checksum of the transaction.
     * @param commitTimestamp commit time for transaction with {@code transactionId}.
     */
    public void set( long transactionId, long checksum, long commitTimestamp )
    {
        highest.set( new TransactionId( transactionId, checksum, commitTimestamp ) );
    }

    /**
     * @return the currently highest transaction together with its checksum.
     */
    public TransactionId get()
    {
        return highest.get();
    }
}
