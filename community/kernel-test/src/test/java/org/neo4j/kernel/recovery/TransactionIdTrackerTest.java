/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.recovery.TransactionStatus.INCOMPLETE;
import static org.neo4j.kernel.recovery.TransactionStatus.RECOVERABLE;
import static org.neo4j.kernel.recovery.TransactionStatus.ROLLED_BACK;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.storageengine.api.CommandBatch;

class TransactionIdTrackerTest {
    @Test
    void completeTransactionTracking() {
        var transactionIdTracker = new TransactionIdTracker();
        var commandBatch1 = createCommandBatch(1, true, true, false);
        var commandBatch3 = createCommandBatch(3, true, true, false);
        var commandBatch2 = createCommandBatch(2, true, true, false);
        var commandBatch4 = createCommandBatch(4, true, true, false);

        transactionIdTracker.trackBatch(commandBatch1);
        transactionIdTracker.trackBatch(commandBatch3);
        transactionIdTracker.trackBatch(commandBatch2);
        transactionIdTracker.trackBatch(commandBatch4);

        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(1));
        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(2));
        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(3));
        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(4));
    }

    @Test
    void trackMultiChunkedCompletedTransactions() {
        var transactionIdTracker = new TransactionIdTracker();
        // chain of 2
        var commandBatch11 = createCommandBatch(1, true, false, false);
        var commandBatch12 = createCommandBatch(1, false, true, false);
        // chain of 2
        var commandBatch21 = createCommandBatch(2, true, false, false);
        var commandBatch22 = createCommandBatch(2, false, true, false);
        // chain of 3
        var commandBatch31 = createCommandBatch(3, true, false, false);
        var commandBatch32 = createCommandBatch(3, false, false, false);
        var commandBatch33 = createCommandBatch(3, false, true, false);

        transactionIdTracker.trackBatch(commandBatch33);
        transactionIdTracker.trackBatch(commandBatch22);
        transactionIdTracker.trackBatch(commandBatch21);

        transactionIdTracker.trackBatch(commandBatch12);
        transactionIdTracker.trackBatch(commandBatch11);

        transactionIdTracker.trackBatch(commandBatch32);
        transactionIdTracker.trackBatch(commandBatch31);

        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(1));
        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(2));
        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(3));
    }

    @Test
    void trackMultiChunkedNonCompleteTransactions() {
        var transactionIdTracker = new TransactionIdTracker();
        // non completed chain of 1
        var commandBatch11 = createCommandBatch(1, true, false, false);
        // non completed chain of 1
        var commandBatch21 = createCommandBatch(2, true, false, false);
        // non completed chain of 2
        var commandBatch31 = createCommandBatch(3, true, false, false);
        var commandBatch32 = createCommandBatch(3, false, false, false);

        transactionIdTracker.trackBatch(commandBatch21);

        transactionIdTracker.trackBatch(commandBatch11);

        transactionIdTracker.trackBatch(commandBatch32);
        transactionIdTracker.trackBatch(commandBatch31);

        assertEquals(INCOMPLETE, transactionIdTracker.transactionStatus(1));
        assertEquals(INCOMPLETE, transactionIdTracker.transactionStatus(2));
        assertEquals(INCOMPLETE, transactionIdTracker.transactionStatus(3));
    }

    @Test
    void trackCombinationOfTransactions() {
        var transactionIdTracker = new TransactionIdTracker();
        // completed chain of 1
        var commandBatch11 = createCommandBatch(1, true, true, false);
        // completed chain of 2
        var commandBatch21 = createCommandBatch(2, true, false, false);
        var commandBatch22 = createCommandBatch(2, false, true, false);
        // non completed chain of 2
        var commandBatch31 = createCommandBatch(3, true, false, false);
        var commandBatch32 = createCommandBatch(3, false, false, false);
        // completed chain of 4
        var commandBatch41 = createCommandBatch(4, true, false, false);
        var commandBatch42 = createCommandBatch(4, false, false, false);
        var commandBatch43 = createCommandBatch(4, false, false, false);
        var commandBatch44 = createCommandBatch(4, false, true, false);
        // completed chain of 1
        var commandBatch51 = createCommandBatch(5, true, true, false);
        // non completed chain of 1
        var commandBatch61 = createCommandBatch(6, true, false, false);
        // completed chain of 1
        var commandBatch71 = createCommandBatch(7, true, true, false);

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

        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(1));
        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(2));
        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(4));
        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(5));
        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(7));

        assertEquals(INCOMPLETE, transactionIdTracker.transactionStatus(3));
        assertEquals(INCOMPLETE, transactionIdTracker.transactionStatus(6));
    }

    @Test
    void trackRollbacksOfTransactions() {
        var transactionIdTracker = new TransactionIdTracker();

        // chain of 2
        var commandBatch11 = createCommandBatch(1, true, false, false);
        var commandBatch12 = createCommandBatch(1, false, true, true);
        // chain of 2
        var commandBatch21 = createCommandBatch(2, true, false, false);
        var commandBatch22 = createCommandBatch(2, false, true, false);
        // chain of 3
        var commandBatch31 = createCommandBatch(3, true, false, false);
        var commandBatch32 = createCommandBatch(3, false, false, false);
        var commandBatch33 = createCommandBatch(3, false, true, true);
        // chain of 2
        var commandBatch41 = createCommandBatch(4, true, false, false);
        var commandBatch42 = createCommandBatch(4, false, true, false);

        transactionIdTracker.trackBatch(commandBatch42);
        transactionIdTracker.trackBatch(commandBatch22);
        transactionIdTracker.trackBatch(commandBatch21);

        transactionIdTracker.trackBatch(commandBatch12);
        transactionIdTracker.trackBatch(commandBatch11);

        transactionIdTracker.trackBatch(commandBatch41);

        transactionIdTracker.trackBatch(commandBatch33);
        transactionIdTracker.trackBatch(commandBatch32);

        transactionIdTracker.trackBatch(commandBatch31);

        assertEquals(ROLLED_BACK, transactionIdTracker.transactionStatus(1));
        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(2));
        assertEquals(ROLLED_BACK, transactionIdTracker.transactionStatus(3));
        assertEquals(RECOVERABLE, transactionIdTracker.transactionStatus(4));
    }

    @Test
    void trackNonCompletedTransactions() {
        var transactionIdTracker = new TransactionIdTracker();

        // chain of 2
        var commandBatch11 = createCommandBatch(1, true, false, false);
        var commandBatch12 = createCommandBatch(1, false, true, true);
        // chain of 2
        var commandBatch21 = createCommandBatch(2, true, false, false);
        var commandBatch22 = createCommandBatch(2, false, false, false);
        // chain of 3
        var commandBatch31 = createCommandBatch(3, true, false, false);
        var commandBatch32 = createCommandBatch(3, false, false, false);
        var commandBatch33 = createCommandBatch(3, false, true, true);
        // chain of 2
        var commandBatch41 = createCommandBatch(4, true, false, false);
        var commandBatch42 = createCommandBatch(4, false, true, false);

        var commandBatch5 = createCommandBatch(5, true, false, false);

        transactionIdTracker.trackBatch(commandBatch5);
        transactionIdTracker.trackBatch(commandBatch42);
        transactionIdTracker.trackBatch(commandBatch41);

        transactionIdTracker.trackBatch(commandBatch33);
        transactionIdTracker.trackBatch(commandBatch32);
        transactionIdTracker.trackBatch(commandBatch31);

        transactionIdTracker.trackBatch(commandBatch22);
        transactionIdTracker.trackBatch(commandBatch21);
        transactionIdTracker.trackBatch(commandBatch12);
        transactionIdTracker.trackBatch(commandBatch11);

        assertThat(transactionIdTracker.notCompletedTransactions()).containsExactly(2, 5);
    }

    private static CommittedCommandBatch createCommandBatch(long id, boolean first, boolean last, boolean rollback) {
        var committedCommandBatch = mock(CommittedCommandBatch.class);
        var commandBatch = mock(CommandBatch.class);
        when(commandBatch.isFirst()).thenReturn(first);
        when(commandBatch.isLast()).thenReturn(last);
        when(committedCommandBatch.txId()).thenReturn(id);
        when(committedCommandBatch.isRollback()).thenReturn(rollback);
        when(committedCommandBatch.commandBatch()).thenReturn(commandBatch);
        return committedCommandBatch;
    }
}
