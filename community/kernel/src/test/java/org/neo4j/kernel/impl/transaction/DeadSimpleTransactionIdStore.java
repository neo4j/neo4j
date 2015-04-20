/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.ArrayQueueOutOfOrderSequence;
import org.neo4j.kernel.impl.util.OutOfOrderSequence;

/**
 * Duplicates the {@link TransactionIdStore} parts of {@link NeoStore}, which is somewhat bad to have to keep
 * in sync.
 */
public class DeadSimpleTransactionIdStore implements TransactionIdStore
{
    private final AtomicLong committingTransactionId = new AtomicLong();
    private final OutOfOrderSequence committedTransactionId = new ArrayQueueOutOfOrderSequence( -1, 100 );
    private final OutOfOrderSequence closedTransactionId = new ArrayQueueOutOfOrderSequence( -1, 100 );
    private final long previouslyCommittedTxId;
    private final long initialTransactionChecksum;

    public DeadSimpleTransactionIdStore()
    {
        this( TransactionIdStore.BASE_TX_ID, 0 );
    }

    public DeadSimpleTransactionIdStore( long previouslyCommittedTxId, long checksum )
    {
        setLastCommittedAndClosedTransactionId( previouslyCommittedTxId, checksum );
        this.previouslyCommittedTxId = previouslyCommittedTxId;
        this.initialTransactionChecksum = checksum;
    }

    @Override
    public long nextCommittingTransactionId()
    {
        return committingTransactionId.incrementAndGet();
    }

    @Override
    public void transactionCommitted( long transactionId, long checksum )
    {
        committedTransactionId.offer( transactionId, checksum );
    }

    @Override
    public long getLastCommittedTransactionId()
    {
        return committedTransactionId.getHighestGapFreeNumber();
    }

    @Override
    public long[] getLastCommittedTransaction()
    {
        return committedTransactionId.get();
    }

    @Override
    public long[] getUpgradeTransaction()
    {
        return new long[] {previouslyCommittedTxId, initialTransactionChecksum};
    }

    @Override
    public long getLastClosedTransactionId()
    {
        return closedTransactionId.getHighestGapFreeNumber();
    }

    @Override
    public void setLastCommittedAndClosedTransactionId( long transactionId, long checksum )
    {
        committingTransactionId.set( transactionId );
        committedTransactionId.set( transactionId, checksum );
        closedTransactionId.set( transactionId, checksum );
    }

    @Override
    public void transactionClosed( long transactionId )
    {
        closedTransactionId.offer( transactionId, 0 );
    }

    @Override
    public boolean closedTransactionIdIsOnParWithOpenedTransactionId()
    {
        return closedTransactionId.getHighestGapFreeNumber() == committedTransactionId.getHighestGapFreeNumber();
    }

    @Override
    public void flush()
    {
    }
}
