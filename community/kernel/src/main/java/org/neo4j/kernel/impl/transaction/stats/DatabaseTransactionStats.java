/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.stats;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.neo4j.kernel.impl.transaction.TransactionMonitor;

public class DatabaseTransactionStats implements TransactionMonitor, TransactionCounters
{
    private final AtomicLong activeReadTransactionCount = new AtomicLong();
    private final LongAdder startedTransactionCount = new LongAdder();
    private final LongAdder activeWriteTransactionCount = new LongAdder();
    private final LongAdder committedReadTransactionCount = new LongAdder();
    private final LongAdder committedWriteTransactionCount = new LongAdder();
    private final LongAdder rolledBackReadTransactionCount = new LongAdder();
    private final LongAdder rolledBackWriteTransactionCount = new LongAdder();
    private final LongAdder terminatedReadTransactionCount = new LongAdder();
    private final LongAdder terminatedWriteTransactionCount = new LongAdder();
    private volatile long peakTransactionCount;

    @Override
    public void transactionStarted()
    {
        startedTransactionCount.increment();
        long active = activeReadTransactionCount.incrementAndGet();
        peakTransactionCount = Math.max( peakTransactionCount, active );
    }

    @Override
    public void transactionFinished( boolean committed, boolean write )
    {
        if ( write )
        {
            activeWriteTransactionCount.decrement();
        }
        else
        {
            activeReadTransactionCount.decrementAndGet();
        }
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
        activeReadTransactionCount.decrementAndGet();
        activeWriteTransactionCount.increment();
    }

    @Override
    public long getPeakConcurrentNumberOfTransactions()
    {
        return peakTransactionCount;
    }

    @Override
    public long getNumberOfStartedTransactions()
    {
        return startedTransactionCount.longValue();
    }

    @Override
    public long getNumberOfCommittedTransactions()
    {
        return getNumberOfCommittedReadTransactions() + getNumberOfCommittedWriteTransactions();
    }

    @Override
    public long getNumberOfCommittedReadTransactions()
    {
        return committedReadTransactionCount.longValue();
    }

    @Override
    public long getNumberOfCommittedWriteTransactions()
    {
        return committedWriteTransactionCount.longValue();
    }

    @Override
    public long getNumberOfActiveTransactions()
    {
        return getNumberOfActiveReadTransactions() + getNumberOfActiveWriteTransactions();
    }

    @Override
    public long getNumberOfActiveReadTransactions()
    {
        return activeReadTransactionCount.longValue();
    }

    @Override
    public long getNumberOfActiveWriteTransactions()
    {
        return activeWriteTransactionCount.longValue();
    }

    @Override
    public long getNumberOfTerminatedTransactions()
    {
        return getNumberOfTerminatedReadTransactions() + getNumberOfTerminatedWriteTransactions();
    }

    @Override
    public long getNumberOfTerminatedReadTransactions()
    {
        return terminatedReadTransactionCount.longValue();
    }

    @Override
    public long getNumberOfTerminatedWriteTransactions()
    {
        return terminatedWriteTransactionCount.longValue();
    }

    @Override
    public long getNumberOfRolledBackTransactions()
    {
        return getNumberOfRolledBackReadTransactions() + getNumberOfRolledBackWriteTransactions();
    }

    @Override
    public long getNumberOfRolledBackReadTransactions()
    {
        return rolledBackReadTransactionCount.longValue();
    }

    @Override
    public long getNumberOfRolledBackWriteTransactions()
    {
        return rolledBackWriteTransactionCount.longValue();
    }

    private static void incrementCounter( LongAdder readCount, LongAdder writeCount, boolean write )
    {
        if ( write )
        {
            writeCount.increment();
        }
        else
        {
            readCount.increment();
        }
    }
}
