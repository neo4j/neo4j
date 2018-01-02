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
package org.neo4j.kernel.impl.transaction;

import java.util.concurrent.atomic.AtomicLong;

public class TransactionCounters implements TransactionMonitor
{
    private final AtomicLong startedTransactionCount = new AtomicLong();
    private final AtomicLong activeTransactionCount = new AtomicLong();
    private final AtomicLong rolledBackTransactionCount = new AtomicLong();
    private final AtomicLong terminatedTransactionCount = new AtomicLong();
    private long peakTransactionCount; // hard to have absolutely atomic, and it doesn't need to be.

    @Override
    public void transactionStarted()
    {
        // TODO offload stats keeping somehow from executing thread?
        startedTransactionCount.incrementAndGet();
        long active = activeTransactionCount.incrementAndGet();
        peakTransactionCount = Math.max( peakTransactionCount, active );
    }

    @Override
    public void transactionFinished( boolean successful )
    {
        long count = activeTransactionCount.decrementAndGet();
        assert count >= 0;
        if ( !successful )
        {
            rolledBackTransactionCount.incrementAndGet();
        }
    }

    @Override
    public void transactionTerminated()
    {
        terminatedTransactionCount.incrementAndGet();
    }

    public long getNumberOfActiveTransactions()
    {
        return activeTransactionCount.get();
    }

    public long getPeakConcurrentNumberOfTransactions()
    {
        return peakTransactionCount;
    }

    public long getNumberOfStartedTransactions()
    {
        return startedTransactionCount.get();
    }

    public long getNumberOfCommittedTransactions()
    {
        return startedTransactionCount.get()
                - activeTransactionCount.get()
                - rolledBackTransactionCount.get()
                - terminatedTransactionCount.get();
    }

    public long getNumberOfTerminatedTransactions()
    {
        return terminatedTransactionCount.get();
    }

    public long getNumberOfRolledbackTransactions()
    {
        return rolledBackTransactionCount.get();
    }
}
