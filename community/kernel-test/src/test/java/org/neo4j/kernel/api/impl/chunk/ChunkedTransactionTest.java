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
package org.neo4j.kernel.api.impl.chunk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_CHUNK_ID;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;

import java.util.List;
import java.util.function.LongConsumer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.chunk.ChunkMetadata;
import org.neo4j.kernel.impl.api.chunk.ChunkedCommandBatch;
import org.neo4j.kernel.impl.api.chunk.ChunkedTransaction;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.storageengine.api.Commitment;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class ChunkedTransactionTest {
    @Test
    void doNotCallCloseListenerOnNonLastChunk() {
        var closedCallback = new CallsCountingConsumer();
        try (var transaction = new ChunkedTransaction(
                CursorContext.NULL_CONTEXT,
                1,
                StoreCursors.NULL,
                Commitment.NO_COMMITMENT,
                TransactionIdGenerator.EMPTY)) {
            var chunkMetadata = new ChunkMetadata(
                    true,
                    false,
                    false,
                    UNKNOWN_APPEND_INDEX,
                    BASE_CHUNK_ID,
                    new MutableLong(1),
                    new MutableLong(2),
                    1,
                    2,
                    3,
                    4,
                    LATEST_KERNEL_VERSION,
                    AUTH_DISABLED);

            transaction.init(new ChunkedCommandBatch(List.of(), chunkMetadata));
            transaction.onClose(closedCallback);
        }

        assertThat(closedCallback.getInvocationCount()).isZero();
    }

    @Test
    void callListenerOnTheLastChunk() {
        var closedCallback = new CallsCountingConsumer();
        try (var transaction = new ChunkedTransaction(
                CursorContext.NULL_CONTEXT,
                1,
                StoreCursors.NULL,
                Commitment.NO_COMMITMENT,
                TransactionIdGenerator.EMPTY)) {
            var chunkMetadata = new ChunkMetadata(
                    false,
                    true,
                    false,
                    UNKNOWN_APPEND_INDEX,
                    BASE_CHUNK_ID,
                    new MutableLong(1),
                    new MutableLong(2),
                    1,
                    2,
                    3,
                    4,
                    LATEST_KERNEL_VERSION,
                    AUTH_DISABLED);

            transaction.init(new ChunkedCommandBatch(List.of(), chunkMetadata));
            transaction.onClose(closedCallback);
        }

        assertThat(closedCallback.getInvocationCount()).isOne();
    }

    private static class CallsCountingConsumer implements LongConsumer {
        private int invocationCount = 0;

        @Override
        public void accept(long value) {
            invocationCount++;
        }

        public int getInvocationCount() {
            return invocationCount;
        }
    }
}
