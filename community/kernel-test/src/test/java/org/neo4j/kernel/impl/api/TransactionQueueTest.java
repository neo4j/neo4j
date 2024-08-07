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
package org.neo4j.kernel.impl.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.api.TransactionQueue.Applier;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.Commitment;
import org.neo4j.storageengine.api.cursor.StoreCursors;

class TransactionQueueTest {
    @Test
    void shouldEmptyIfTooMany() throws Exception {
        // GIVEN
        Applier applier = mock(Applier.class);
        int batchSize = 10;
        TransactionQueue queue = new TransactionQueue(batchSize, applier);

        // WHEN
        for (int i = 0; i < 9; i++) {
            queue.queue(mock(CompleteTransaction.class));
            verifyNoMoreInteractions(applier);
        }
        queue.queue(mock(CompleteTransaction.class));
        verify(applier).apply(any());
        reset(applier);

        // THEN
        queue.queue(mock(CompleteTransaction.class));

        // and WHEN emptying in the end
        for (int i = 0; i < 2; i++) {
            queue.queue(mock(CompleteTransaction.class));
            verifyNoMoreInteractions(applier);
        }
        queue.applyTransactions();
        verify(applier).apply(any());
    }

    @Test
    void shouldLinkTogetherTransactions() throws Exception {
        // GIVEN
        Applier applier = mock(Applier.class);
        int batchSize = 10;
        TransactionQueue queue = new TransactionQueue(batchSize, applier);

        // WHEN
        CompleteTransaction[] txs = new CompleteTransaction[batchSize];
        for (int i = 0; i < batchSize; i++) {
            queue.queue(
                    txs[i] = new CompleteTransaction(
                            mock(CommandBatch.class),
                            NULL_CONTEXT,
                            StoreCursors.NULL,
                            Commitment.NO_COMMITMENT,
                            TransactionIdGenerator.EMPTY));
        }

        // THEN
        verify(applier).apply(any());
        for (int i = 0; i < txs.length - 1; i++) {
            assertEquals(txs[i + 1], txs[i].next());
        }
    }
}
