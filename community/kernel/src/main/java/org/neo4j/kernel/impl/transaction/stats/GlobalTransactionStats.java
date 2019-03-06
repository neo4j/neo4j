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
package org.neo4j.kernel.impl.transaction.stats;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.ToLongFunction;

public class GlobalTransactionStats implements TransactionCounters
{
    private final CopyOnWriteArrayList<TransactionCounters> databasesCounters = new CopyOnWriteArrayList<>();

    /**
     * Peak concurrent number of transaction is only supported on a database level.
     */
    @Override
    public long getPeakConcurrentNumberOfTransactions()
    {
        return -1;
    }

    @Override
    public long getNumberOfStartedTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfStartedTransactions );
    }

    @Override
    public long getNumberOfCommittedTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfCommittedTransactions );
    }

    @Override
    public long getNumberOfCommittedReadTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfCommittedReadTransactions );
    }

    @Override
    public long getNumberOfCommittedWriteTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfCommittedWriteTransactions );
    }

    @Override
    public long getNumberOfActiveTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfActiveTransactions );
    }

    @Override
    public long getNumberOfActiveReadTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfActiveReadTransactions );
    }

    @Override
    public long getNumberOfActiveWriteTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfActiveWriteTransactions );
    }

    @Override
    public long getNumberOfTerminatedTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfTerminatedTransactions );
    }

    @Override
    public long getNumberOfTerminatedReadTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfTerminatedReadTransactions );
    }

    @Override
    public long getNumberOfTerminatedWriteTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfTerminatedWriteTransactions );
    }

    @Override
    public long getNumberOfRolledBackTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfRolledBackTransactions );
    }

    @Override
    public long getNumberOfRolledBackReadTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfRolledBackReadTransactions );
    }

    @Override
    public long getNumberOfRolledBackWriteTransactions()
    {
        return sumCounters( TransactionCounters::getNumberOfRolledBackWriteTransactions );
    }

    public DatabaseTransactionStats createDatabaseTransactionMonitor()
    {
        DatabaseTransactionStats transactionStats = new DatabaseTransactionStats();
        databasesCounters.add( transactionStats );
        return transactionStats;
    }

    private long sumCounters( ToLongFunction<TransactionCounters> mappingFunction )
    {
        return databasesCounters.stream().mapToLong( mappingFunction ).sum();
    }
}
