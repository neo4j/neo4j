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

import static org.junit.Assert.assertEquals;

class TransactionCountersChecker
{
    private final long numberOfActiveReadTransactions;
    private final long numberOfActiveWriteTransactions;
    private final long numberOfActiveTransactions;
    private final long numberOfCommittedReadTransactions;
    private final long numberOfCommittedWriteTransactions;
    private final long numberOfCommittedTransactions;
    private final long numberOfRolledBackReadTransactions;
    private final long numberOfRolledBackWriteTransactions;
    private final long numberOfRolledBackTransactions;
    private final long numberOfStartedTransactions;
    private final long numberOfTerminatedReadTransactions;
    private final long numberOfTerminatedWriteTransactions;
    private final long numberOfTerminatedTransactions;
    private final long peakConcurrentNumberOfTransactions;

    TransactionCountersChecker( TransactionCounters pre )
    {
        // Active
        numberOfActiveReadTransactions = pre.getNumberOfActiveReadTransactions();
        numberOfActiveWriteTransactions = pre.getNumberOfActiveWriteTransactions();
        numberOfActiveTransactions = pre.getNumberOfActiveTransactions();

        assertEquals( numberOfActiveTransactions,
                numberOfActiveReadTransactions + numberOfActiveWriteTransactions );

        // Committed
        numberOfCommittedReadTransactions = pre.getNumberOfCommittedReadTransactions();
        numberOfCommittedWriteTransactions = pre.getNumberOfCommittedWriteTransactions();
        numberOfCommittedTransactions = pre.getNumberOfCommittedTransactions();

        assertEquals( numberOfCommittedTransactions,
                numberOfCommittedReadTransactions + numberOfCommittedWriteTransactions );

        // RolledBack
        numberOfRolledBackReadTransactions = pre.getNumberOfRolledBackReadTransactions();
        numberOfRolledBackWriteTransactions = pre.getNumberOfRolledBackWriteTransactions();
        numberOfRolledBackTransactions = pre.getNumberOfRolledBackTransactions();

        assertEquals( numberOfRolledBackTransactions,
                numberOfRolledBackReadTransactions + numberOfRolledBackWriteTransactions );

        // Terminated
        numberOfTerminatedReadTransactions = pre.getNumberOfTerminatedReadTransactions();
        numberOfTerminatedWriteTransactions = pre.getNumberOfTerminatedWriteTransactions();
        numberOfTerminatedTransactions = pre.getNumberOfTerminatedTransactions();

        assertEquals( numberOfTerminatedTransactions,
                numberOfTerminatedReadTransactions + numberOfTerminatedWriteTransactions );

        // started
        numberOfStartedTransactions = pre.getNumberOfStartedTransactions();

        // peak
        peakConcurrentNumberOfTransactions = pre.getPeakConcurrentNumberOfTransactions();
    }

    public void verifyCommitted( boolean isWriteTx, TransactionCounters post )
    {
        verifyActiveAndStarted( post );
        verifyCommittedIncreasedBy( post, 1, isWriteTx );
        verifyRolledBackIncreasedBy( post, 0, isWriteTx );
        verifyTerminatedIncreasedBy( post, 0, isWriteTx );
    }

    public void verifyRolledBacked( boolean isWriteTx, TransactionCounters post )
    {
        verifyActiveAndStarted( post );
        verifyCommittedIncreasedBy( post, 0, isWriteTx );
        verifyRolledBackIncreasedBy( post, 1, isWriteTx );
        verifyTerminatedIncreasedBy( post, 0, isWriteTx );
    }

    public void verifyTerminated( boolean isWriteTx, TransactionCounters post )
    {
        verifyActiveAndStarted( post );
        verifyCommittedIncreasedBy( post, 0, isWriteTx );
        verifyRolledBackIncreasedBy( post, 1, isWriteTx );
        verifyTerminatedIncreasedBy( post, 1, isWriteTx );
    }

    private void verifyCommittedIncreasedBy( TransactionCounters post, int diff, boolean isWriteTx )
    {
        if ( isWriteTx )
        {
            assertEquals( numberOfCommittedReadTransactions, post.getNumberOfCommittedReadTransactions() );
            assertEquals( numberOfCommittedWriteTransactions + diff,
                    post.getNumberOfCommittedWriteTransactions() );
        }
        else
        {
            assertEquals( numberOfCommittedReadTransactions + diff,
                    post.getNumberOfCommittedReadTransactions() );
            assertEquals( numberOfCommittedWriteTransactions, post.getNumberOfCommittedWriteTransactions() );
        }

        assertEquals( numberOfCommittedTransactions + diff, post.getNumberOfCommittedTransactions() );
    }

    private void verifyRolledBackIncreasedBy( TransactionCounters post, int diff, boolean isWriteTx )
    {
        if ( isWriteTx )
        {
            assertEquals( numberOfRolledBackReadTransactions, post.getNumberOfRolledBackReadTransactions() );
            assertEquals( numberOfRolledBackWriteTransactions + diff,
                    post.getNumberOfRolledBackWriteTransactions() );
        }
        else
        {
            assertEquals( numberOfRolledBackReadTransactions + diff,
                    post.getNumberOfRolledBackReadTransactions() );
            assertEquals( numberOfRolledBackWriteTransactions, post.getNumberOfRolledBackWriteTransactions() );
        }

        assertEquals( numberOfRolledBackTransactions + diff, post.getNumberOfRolledBackTransactions() );
    }

    private void verifyTerminatedIncreasedBy( TransactionCounters post, int diff, boolean isWriteTx )
    {
        if ( isWriteTx )
        {
            assertEquals( numberOfTerminatedReadTransactions, post.getNumberOfTerminatedReadTransactions() );
            assertEquals( numberOfTerminatedWriteTransactions + diff,
                    post.getNumberOfTerminatedWriteTransactions() );
        }
        else
        {
            assertEquals( numberOfTerminatedReadTransactions + diff,
                    post.getNumberOfTerminatedReadTransactions() );
            assertEquals( numberOfTerminatedWriteTransactions, post.getNumberOfTerminatedWriteTransactions() );
        }

        assertEquals( numberOfTerminatedTransactions + diff, post.getNumberOfTerminatedTransactions() );
    }

    private void verifyActiveAndStarted( TransactionCounters post )
    {
        assertEquals( numberOfActiveReadTransactions, post.getNumberOfActiveReadTransactions() );
        assertEquals( numberOfActiveWriteTransactions, post.getNumberOfActiveWriteTransactions() );
        assertEquals( numberOfActiveTransactions, post.getNumberOfActiveTransactions() );

        assertEquals( numberOfStartedTransactions + 1, post.getNumberOfStartedTransactions() );
        assertEquals( Math.max( 1, peakConcurrentNumberOfTransactions ),
                post.getPeakConcurrentNumberOfTransactions() );
    }
}
