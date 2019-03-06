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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.transaction.TransactionMonitor;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GlobalTransactionStatsTest
{
    private TransactionMonitor monitor1;
    private TransactionMonitor monitor2;
    private GlobalTransactionStats stats;

    @BeforeEach
    void setUp()
    {
        stats = new GlobalTransactionStats();
        monitor1 = stats.createDatabaseTransactionMonitor();
        monitor2 = stats.createDatabaseTransactionMonitor();
    }

    @Test
    void getNumberOfStartedTransactions()
    {
        monitor1.transactionStarted();
        monitor1.transactionStarted();
        monitor2.transactionStarted();

        assertEquals( 3, stats.getNumberOfStartedTransactions() );
    }

    @Test
    void getNumberOfCommittedTransactions()
    {
        monitor1.transactionStarted();
        monitor1.transactionStarted();
        monitor2.transactionStarted();

        monitor1.transactionFinished( true, true );
        monitor1.transactionFinished( true, false );
        monitor2.transactionFinished( true, true );

        assertEquals( 3, stats.getNumberOfCommittedTransactions() );
    }

    @Test
    void getNumberOfCommittedReadTransactions()
    {
        monitor1.transactionStarted();
        monitor1.transactionStarted();
        monitor2.transactionStarted();

        monitor1.transactionFinished( true, true );
        monitor1.transactionFinished( true, false );
        monitor2.transactionFinished( true, true );

        assertEquals( 1, stats.getNumberOfCommittedReadTransactions() );
    }

    @Test
    void getNumberOfCommittedWriteTransactions()
    {
        monitor1.transactionStarted();
        monitor1.transactionStarted();
        monitor2.transactionStarted();

        monitor1.transactionFinished( true, true );
        monitor1.transactionFinished( false, true );
        monitor2.transactionFinished( true, true );

        assertEquals( 2, stats.getNumberOfCommittedWriteTransactions() );
    }

    @Test
    void getNumberOfActiveTransactions()
    {
        monitor1.transactionStarted();
        monitor1.transactionStarted();
        monitor2.transactionStarted();

        monitor2.upgradeToWriteTransaction();

        assertEquals( 3, stats.getNumberOfActiveTransactions() );
    }

    @Test
    void getNumberOfActiveReadTransactions()
    {
        monitor1.transactionStarted();
        monitor1.transactionStarted();
        monitor2.transactionStarted();

        assertEquals( 3, stats.getNumberOfActiveReadTransactions() );
    }

    @Test
    void getNumberOfActiveWriteTransactions()
    {
        monitor1.transactionStarted();
        monitor1.transactionStarted();
        monitor2.transactionStarted();

        monitor1.upgradeToWriteTransaction();
        monitor2.upgradeToWriteTransaction();

        assertEquals( 2, stats.getNumberOfActiveWriteTransactions() );
    }

    @Test
    void getNumberOfTerminatedTransactions()
    {
        monitor1.transactionStarted();
        monitor1.transactionStarted();
        monitor2.transactionStarted();

        monitor1.transactionTerminated( false );
        monitor1.transactionTerminated( true );
        monitor2.transactionTerminated( false );

        assertEquals( 3, stats.getNumberOfTerminatedTransactions() );
    }

    @Test
    void getNumberOfTerminatedReadTransactions()
    {
        monitor1.transactionStarted();
        monitor1.transactionStarted();
        monitor2.transactionStarted();

        monitor1.transactionTerminated( false );
        monitor1.transactionTerminated( true );
        monitor2.transactionTerminated( false );

        assertEquals( 2, stats.getNumberOfTerminatedReadTransactions() );
    }

    @Test
    void getNumberOfTerminatedWriteTransactions()
    {
        monitor1.transactionStarted();
        monitor2.transactionStarted();

        monitor1.transactionTerminated( true );
        monitor2.transactionTerminated( true );

        assertEquals( 2, stats.getNumberOfTerminatedWriteTransactions() );
    }

    @Test
    void getNumberOfRolledBackTransactions()
    {
        monitor1.transactionStarted();
        monitor1.transactionStarted();
        monitor2.transactionStarted();
        monitor2.transactionStarted();

        monitor1.transactionFinished( false, false );
        monitor1.transactionFinished( false, true );
        monitor2.transactionFinished( false, false );
        monitor2.transactionFinished( false, true );

        assertEquals( 4, stats.getNumberOfRolledBackTransactions() );
    }

    @Test
    void getNumberOfRolledBackReadTransactions()
    {
        monitor1.transactionStarted();
        monitor1.transactionStarted();
        monitor2.transactionStarted();

        monitor1.transactionFinished( false, false );
        monitor2.transactionFinished( false, false );

        assertEquals( 2, stats.getNumberOfRolledBackReadTransactions() );
    }

    @Test
    void getNumberOfRolledBackWriteTransactions()
    {
        monitor1.transactionStarted();
        monitor1.transactionStarted();
        monitor2.transactionStarted();

        monitor1.transactionFinished( false, true );
        monitor1.transactionFinished( false, true );
        monitor2.transactionFinished( false, true );

        assertEquals( 3, stats.getNumberOfRolledBackWriteTransactions() );
    }
}
