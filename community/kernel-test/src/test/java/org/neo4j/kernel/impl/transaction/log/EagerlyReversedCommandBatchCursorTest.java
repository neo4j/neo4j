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
package org.neo4j.kernel.impl.transaction.log;

import static java.util.Collections.emptyList;
import static org.apache.commons.io.IOUtils.EMPTY_BYTE_ARRAY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.array;
import static org.neo4j.kernel.impl.transaction.log.GivenCommandBatchCursor.exhaust;
import static org.neo4j.kernel.impl.transaction.log.GivenCommandBatchCursor.given;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newCommitEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.reverse.EagerlyReversedCommandBatchCursor;

class EagerlyReversedCommandBatchCursorTest {
    @Test
    void shouldReverseTransactionsFromSource() throws Exception {
        // GIVEN
        CommittedTransactionRepresentation tx1 = createTransaction(1);
        CommittedTransactionRepresentation tx2 = createTransaction(2);
        CommittedTransactionRepresentation tx3 = createTransaction(3);
        CommandBatchCursor source = given(tx1, tx2, tx3);
        EagerlyReversedCommandBatchCursor cursor = new EagerlyReversedCommandBatchCursor(source);

        // WHEN
        CommittedCommandBatch[] reversed = exhaust(cursor);

        // THEN
        assertArrayEquals(array(tx3, tx2, tx1), reversed);
    }

    @Test
    void shouldHandleEmptySource() throws Exception {
        // GIVEN
        CommandBatchCursor source = given();
        EagerlyReversedCommandBatchCursor cursor = new EagerlyReversedCommandBatchCursor(source);

        // WHEN
        CommittedCommandBatch[] reversed = exhaust(cursor);

        // THEN
        assertEquals(0, reversed.length);
    }

    private static CommittedTransactionRepresentation createTransaction(long txId) {
        return new CommittedTransactionRepresentation(
                newStartEntry(LATEST_KERNEL_VERSION, 1, 2, 3, 4, EMPTY_BYTE_ARRAY, LogPosition.UNSPECIFIED),
                emptyList(),
                newCommitEntry(LATEST_KERNEL_VERSION, txId, 1L, BASE_TX_CHECKSUM));
    }
}
