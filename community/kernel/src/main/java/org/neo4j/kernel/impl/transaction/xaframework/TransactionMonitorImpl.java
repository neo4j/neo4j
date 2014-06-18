/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.util.concurrent.atomic.AtomicInteger;

public class TransactionMonitorImpl implements TransactionMonitor
{
    private final AtomicInteger startedTransactionCount = new AtomicInteger();
    private final AtomicInteger activeTransactionCount = new AtomicInteger();
    private final AtomicInteger rolledBackTransactionCount = new AtomicInteger();
    private int peakTransactionCount; // hard to have absolutely atomic, and it doesn't need to be.

    @Override
    public void transactionStarted()
    {
        // TODO offload stats keeping somehow from executing thread?
        startedTransactionCount.incrementAndGet();
        int active = activeTransactionCount.incrementAndGet();
        peakTransactionCount = Math.max( peakTransactionCount, active );
    }

    @Override
    public void transactionFinished( boolean successful )
    {
        activeTransactionCount.decrementAndGet();
        if ( !successful )
        {
            rolledBackTransactionCount.incrementAndGet();
        }
    }

    @Override
    public int getNumberOfActiveTransactions()
    {
        return activeTransactionCount.get();
    }

    @Override
    public int getPeakConcurrentNumberOfTransactions()
    {
        return peakTransactionCount;
    }

    @Override
    public int getNumberOfStartedTransactions()
    {
        return startedTransactionCount.get();
    }

    @Override
    public long getNumberOfCommittedTransactions()
    {
        return startedTransactionCount.get() - activeTransactionCount.get() - rolledBackTransactionCount.get();
    }

    @Override
    public long getNumberOfRolledbackTransactions()
    {
        return rolledBackTransactionCount.get();
    }
}
