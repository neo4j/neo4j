/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.recovery;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.storageengine.api.CommandBatch;

class TransactionIdTrackerTest {
    @Test
    void completeTransactionTracking() {
        var transactionIdTracker = new TransactionIdTracker();
        var commandBatch1 = createCommandBatch(1, true, true);
        var commandBatch3 = createCommandBatch(3, true, true);
        var commandBatch2 = createCommandBatch(2, true, true);
        var commandBatch4 = createCommandBatch(4, true, true);

        transactionIdTracker.trackBatch(commandBatch1);
        transactionIdTracker.trackBatch(commandBatch3);
        transactionIdTracker.trackBatch(commandBatch2);
        transactionIdTracker.trackBatch(commandBatch4);

        assertTrue(transactionIdTracker.isCompletedTransaction(1));
        assertTrue(transactionIdTracker.isCompletedTransaction(2));
        assertTrue(transactionIdTracker.isCompletedTransaction(3));
        assertTrue(transactionIdTracker.isCompletedTransaction(4));
    }

    @Test
    void trackMultiChunkedCompletedTransactions() {
        var transactionIdTracker = new TransactionIdTracker();
        // chain of 2
        var commandBatch11 = createCommandBatch(1, true, false);
        var commandBatch12 = createCommandBatch(1, false, true);
        // chain of 2
        var commandBatch21 = createCommandBatch(2, true, false);
        var commandBatch22 = createCommandBatch(2, false, true);
        // chain of 3
        var commandBatch31 = createCommandBatch(3, true, false);
        var commandBatch32 = createCommandBatch(3, false, false);
        var commandBatch33 = createCommandBatch(3, false, true);

        transactionIdTracker.trackBatch(commandBatch33);
        transactionIdTracker.trackBatch(commandBatch22);
        transactionIdTracker.trackBatch(commandBatch21);

        transactionIdTracker.trackBatch(commandBatch12);
        transactionIdTracker.trackBatch(commandBatch11);

        transactionIdTracker.trackBatch(commandBatch32);
        transactionIdTracker.trackBatch(commandBatch31);

        assertTrue(transactionIdTracker.isCompletedTransaction(1));
        assertTrue(transactionIdTracker.isCompletedTransaction(2));
        assertTrue(transactionIdTracker.isCompletedTransaction(3));
    }

    @Test
    void trackMultiChunkedNonCompleteTransactions() {
        var transactionIdTracker = new TransactionIdTracker();
        // non completed chain of 1
        var commandBatch11 = createCommandBatch(1, true, false);
        // non completed chain of 1
        var commandBatch21 = createCommandBatch(2, true, false);
        // non completed chain of 2
        var commandBatch31 = createCommandBatch(3, true, false);
        var commandBatch32 = createCommandBatch(3, false, false);

        transactionIdTracker.trackBatch(commandBatch21);

        transactionIdTracker.trackBatch(commandBatch11);

        transactionIdTracker.trackBatch(commandBatch32);
        transactionIdTracker.trackBatch(commandBatch31);

        assertFalse(transactionIdTracker.isCompletedTransaction(1));
        assertFalse(transactionIdTracker.isCompletedTransaction(2));
        assertFalse(transactionIdTracker.isCompletedTransaction(3));
    }

    @Test
    void trackCombinationOfTransactions() {
        var transactionIdTracker = new TransactionIdTracker();
        // completed chain of 1
        var commandBatch11 = createCommandBatch(1, true, true);
        // completed chain of 2
        var commandBatch21 = createCommandBatch(2, true, false);
        var commandBatch22 = createCommandBatch(2, false, true);
        // non completed chain of 2
        var commandBatch31 = createCommandBatch(3, true, false);
        var commandBatch32 = createCommandBatch(3, false, false);
        // completed chain of 4
        var commandBatch41 = createCommandBatch(4, true, false);
        var commandBatch42 = createCommandBatch(4, false, false);
        var commandBatch43 = createCommandBatch(4, false, false);
        var commandBatch44 = createCommandBatch(4, false, true);
        // completed chain of 1
        var commandBatch51 = createCommandBatch(5, true, true);
        // non completed chain of 1
        var commandBatch61 = createCommandBatch(6, true, false);
        // completed chain of 1
        var commandBatch71 = createCommandBatch(7, true, true);

        transactionIdTracker.trackBatch(commandBatch71);
        transactionIdTracker.trackBatch(commandBatch44);
        transactionIdTracker.trackBatch(commandBatch32);
        transactionIdTracker.trackBatch(commandBatch22);
        transactionIdTracker.trackBatch(commandBatch11);

        transactionIdTracker.trackBatch(commandBatch51);
        transactionIdTracker.trackBatch(commandBatch61);

        transactionIdTracker.trackBatch(commandBatch43);
        transactionIdTracker.trackBatch(commandBatch42);

        transactionIdTracker.trackBatch(commandBatch31);
        transactionIdTracker.trackBatch(commandBatch41);

        assertTrue(transactionIdTracker.isCompletedTransaction(1));
        assertTrue(transactionIdTracker.isCompletedTransaction(2));
        assertTrue(transactionIdTracker.isCompletedTransaction(4));
        assertTrue(transactionIdTracker.isCompletedTransaction(5));
        assertTrue(transactionIdTracker.isCompletedTransaction(7));

        assertFalse(transactionIdTracker.isCompletedTransaction(3));
        assertFalse(transactionIdTracker.isCompletedTransaction(6));
    }

    private static CommittedCommandBatch createCommandBatch(long id, boolean first, boolean last) {
        var committedCommandBatch = mock(CommittedCommandBatch.class);
        var commandBatch = mock(CommandBatch.class);
        when(commandBatch.isFirst()).thenReturn(first);
        when(commandBatch.isLast()).thenReturn(last);
        when(committedCommandBatch.txId()).thenReturn(id);
        when(committedCommandBatch.commandBatch()).thenReturn(commandBatch);
        return committedCommandBatch;
    }
}
