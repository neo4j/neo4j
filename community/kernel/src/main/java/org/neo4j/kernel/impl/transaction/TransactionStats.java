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
package org.neo4j.kernel.impl.transaction;

import java.util.concurrent.atomic.AtomicLong;

public class TransactionStats implements TransactionMonitor, TransactionCounters
{
    private final AtomicLong startedTransactionCount = new AtomicLong();
    private final AtomicLong activeReadTransactionCount = new AtomicLong();
    private final AtomicLong activeWriteTransactionCount = new AtomicLong();
    private final AtomicLong committedReadTransactionCount = new AtomicLong();
    private final AtomicLong committedWriteTransactionCount = new AtomicLong();
    private final AtomicLong rolledBackReadTransactionCount = new AtomicLong();
    private final AtomicLong rolledBackWriteTransactionCount = new AtomicLong();
    private final AtomicLong terminatedReadTransactionCount = new AtomicLong();
    private final AtomicLong terminatedWriteTransactionCount = new AtomicLong();
    private volatile long peakTransactionCount;

    @Override
    public void transactionStarted()
    {
        startedTransactionCount.incrementAndGet();
        long active = activeReadTransactionCount.incrementAndGet();
        peakTransactionCount = Math.max( peakTransactionCount, active );
    }

    @Override
    public void transactionFinished( boolean committed, boolean write )
    {
        decrementCounter( activeReadTransactionCount, activeWriteTransactionCount, write );
        if ( committed )
        {
            incrementCounter( committedReadTransactionCount, committedWriteTransactionCount, write );
        }
        else
        {
            incrementCounter( rolledBackReadTransactionCount, rolledBackWriteTransactionCount, write );
        }
    }

    @Override
    public void transactionTerminated( boolean write )
    {
        incrementCounter( terminatedReadTransactionCount, terminatedWriteTransactionCount, write );
    }

    @Override
    public void upgradeToWriteTransaction()
    {
        long readCount = activeReadTransactionCount.decrementAndGet();
        assert readCount >= 0;
        long writeCount = activeWriteTransactionCount.incrementAndGet();
        assert writeCount > 0;
    }

    @Override
    public long getPeakConcurrentNumberOfTransactions()
    {
        return peakTransactionCount;
    }

    @Override
    public long getNumberOfStartedTransactions()
    {
        return startedTransactionCount.get();
    }

    @Override
    public long getNumberOfCommittedTransactions()
    {
        return getNumberOfCommittedReadTransactions() + getNumberOfCommittedWriteTransactions();
    }

    @Override
    public long getNumberOfCommittedReadTransactions()
    {
        return committedReadTransactionCount.get();
    }

    @Override
    public long getNumberOfCommittedWriteTransactions()
    {
        return committedWriteTransactionCount.get();
    }

    @Override
    public long getNumberOfActiveTransactions()
    {
        return getNumberOfActiveReadTransactions() + getNumberOfActiveWriteTransactions();
    }

    @Override
    public long getNumberOfActiveReadTransactions()
    {
        return activeReadTransactionCount.get();
    }

    @Override
    public long getNumberOfActiveWriteTransactions()
    {
        return activeWriteTransactionCount.get();
    }

    @Override
    public long getNumberOfTerminatedTransactions()
    {
        return getNumberOfTerminatedReadTransactions() + getNumberOfTerminatedWriteTransactions();
    }

    @Override
    public long getNumberOfTerminatedReadTransactions()
    {
        return terminatedReadTransactionCount.get();
    }

    @Override
    public long getNumberOfTerminatedWriteTransactions()
    {
        return terminatedWriteTransactionCount.get();
    }

    @Override
    public long getNumberOfRolledBackTransactions()
    {
        return getNumberOfRolledBackReadTransactions() + getNumberOfRolledBackWriteTransactions();
    }

    @Override
    public long getNumberOfRolledBackReadTransactions()
    {
        return rolledBackReadTransactionCount.get();
    }

    @Override
    public long getNumberOfRolledBackWriteTransactions()
    {
        return rolledBackWriteTransactionCount.get();
    }

    private void incrementCounter( AtomicLong readCount, AtomicLong writeCount, boolean write )
    {
        long count = write ? writeCount.incrementAndGet() : readCount.incrementAndGet();
        assert count > 0;
    }

    private void decrementCounter( AtomicLong readCount, AtomicLong writeCount, boolean write )
    {
        long count = write ? writeCount.decrementAndGet() : readCount.decrementAndGet();
        assert count >= 0;
    }
}
