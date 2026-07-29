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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.neo4j.common.Subject.AUTH_DISABLED;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.LogPositionMetadata.NO_METADATA;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_CHUNK_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_ID;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;

import java.util.List;
import java.util.function.LongConsumer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.availability.MvccIncompleteTransactionAvailabilityService;
import org.neo4j.kernel.impl.api.ChunkedTransactionTracker;
import org.neo4j.kernel.impl.api.chunk.ChunkMetadata;
import org.neo4j.kernel.impl.api.chunk.ChunkedCommandBatch;
import org.neo4j.kernel.impl.api.chunk.ChunkedTransaction;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.Commitment;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class ChunkedTransactionTest {
    private static final long TX_ID = 42;
    private static final int LEASE_ID = 7;
    private static final long CHUNK_COMMIT_TIME = 3;

    private final ChunkedTransactionTracker tracker = new ChunkedTransactionTracker();
    private final MvccIncompleteTransactionAvailabilityService incompleteTransactionAvailability =
            mock(MvccIncompleteTransactionAvailabilityService.class);

    @Test
    void doNotCallCloseListenerOnNonLastChunk() {
        var closedCallback = new CallsCountingConsumer();
        try (var transaction = untrackedTransaction()) {
            transaction.init(chunk(true, false, false, BASE_CHUNK_ID));
            transaction.onClose(closedCallback);
        }

        assertThat(closedCallback.getInvocationCount()).isZero();
    }

    @Test
    void callListenerOnTheLastChunk() {
        var closedCallback = new CallsCountingConsumer();
        try (var transaction = untrackedTransaction()) {
            transaction.init(chunk(false, true, false, BASE_CHUNK_ID));
            transaction.onClose(closedCallback);
        }

        assertThat(closedCallback.getInvocationCount()).isOne();
    }

    @Test
    void registerFirstChunkWithItsOwnAppendIndex() {
        var transaction = trackedFirstChunkTransaction();
        transaction.init(chunk(true, false, false, BASE_CHUNK_ID));

        append(transaction, 5);
        transaction.commit();

        assertThat(tracker.transactionsToRollback())
                .containsExactly(new ChunkedTransactionTracker.TransactionInfo(
                        TX_ID, 5, 5, BASE_CHUNK_ID, LATEST_KERNEL_VERSION, LEASE_ID));
    }

    @Test
    void refreshRegistrationForEveryFollowingChunk() {
        tracker.registerChunkedTransaction(TX_ID, 5, 5, BASE_CHUNK_ID, LATEST_KERNEL_VERSION, LEASE_ID);
        var transaction = trackedLaterChunkTransaction(5);
        transaction.init(chunk(false, false, false, BASE_CHUNK_ID + 1));

        append(transaction, 9);
        transaction.commit();

        assertThat(tracker.transactionsToRollback())
                .containsExactly(new ChunkedTransactionTracker.TransactionInfo(
                        TX_ID, 5, 9, BASE_CHUNK_ID + 1, LATEST_KERNEL_VERSION, LEASE_ID));
    }

    @Test
    void publishLastChunkWithFirstAppendIndexFromTracker() {
        tracker.registerChunkedTransaction(TX_ID, 5, 9, BASE_CHUNK_ID + 1, LATEST_KERNEL_VERSION, LEASE_ID);
        var commitment = mock(Commitment.class);
        var transaction = trackedLaterChunkTransaction(9, commitment);
        transaction.init(chunk(false, true, false, BASE_CHUNK_ID + 2));

        append(transaction, 12);
        transaction.commit();

        verify(commitment).publishAsCommitted(CHUNK_COMMIT_TIME, 5);
    }

    @Test
    void refreshRegistrationForLastChunk() {
        tracker.registerChunkedTransaction(TX_ID, 5, 9, BASE_CHUNK_ID + 1, LATEST_KERNEL_VERSION, LEASE_ID);
        var transaction = trackedLaterChunkTransaction(9);
        transaction.init(chunk(false, true, false, BASE_CHUNK_ID + 2));

        append(transaction, 12);
        transaction.commit();

        assertThat(tracker.transactionsToRollback())
                .containsExactly(new ChunkedTransactionTracker.TransactionInfo(
                        TX_ID, 5, 12, BASE_CHUNK_ID + 2, LATEST_KERNEL_VERSION, LEASE_ID));
    }

    @Test
    void refreshRegistrationForRollbackChunk() {
        tracker.registerChunkedTransaction(TX_ID, 5, 9, BASE_CHUNK_ID + 1, LATEST_KERNEL_VERSION, LEASE_ID);
        var transaction = trackedLaterChunkTransaction(9);
        transaction.init(chunk(false, false, true, BASE_CHUNK_ID + 2));

        append(transaction, 12);
        transaction.commit();

        assertThat(tracker.transactionsToRollback())
                .containsExactly(new ChunkedTransactionTracker.TransactionInfo(
                        TX_ID, 5, 12, BASE_CHUNK_ID + 2, LATEST_KERNEL_VERSION, LEASE_ID));
    }

    /**
     * A transaction that fits in a single chunk is both the first and the last one, so it has to register itself with
     * its own append index rather than looking one up, and it is unregistered again as soon as it closes.
     */
    @Test
    void registerAndCleanupSingleChunkTransaction() {
        var transaction = trackedFirstChunkTransaction();
        transaction.init(chunk(true, true, false, BASE_CHUNK_ID));

        append(transaction, 5);
        transaction.commit();

        assertThat(tracker.transactionsToRollback())
                .containsExactly(new ChunkedTransactionTracker.TransactionInfo(
                        TX_ID, 5, 5, BASE_CHUNK_ID, LATEST_KERNEL_VERSION, LEASE_ID));

        transaction.close();

        assertThat(tracker.transactionsToRollback()).isEmpty();
        verify(incompleteTransactionAvailability).completeTransaction(TX_ID);
    }

    @Test
    void cleanupAndCompleteTransactionOnLastChunk() {
        tracker.registerChunkedTransaction(TX_ID, 5, 9, BASE_CHUNK_ID + 1, LATEST_KERNEL_VERSION, LEASE_ID);
        var transaction = trackedLaterChunkTransaction(9);
        transaction.init(chunk(false, true, false, BASE_CHUNK_ID + 2));

        transaction.close();

        assertThat(tracker.transactionsToRollback()).isEmpty();
        verify(incompleteTransactionAvailability).completeTransaction(TX_ID);
    }

    @Test
    void cleanupAndCompleteTransactionOnRollbackChunk() {
        tracker.registerChunkedTransaction(TX_ID, 5, 9, BASE_CHUNK_ID + 1, LATEST_KERNEL_VERSION, LEASE_ID);
        var transaction = trackedLaterChunkTransaction(9);
        transaction.init(chunk(false, true, true, BASE_CHUNK_ID + 2));

        transaction.close();

        assertThat(tracker.transactionsToRollback()).isEmpty();
        verify(incompleteTransactionAvailability).completeTransaction(TX_ID);
    }

    @Test
    void keepRegistrationOfNonClosingChunkOnClose() {
        tracker.registerChunkedTransaction(TX_ID, 5, 9, BASE_CHUNK_ID + 1, LATEST_KERNEL_VERSION, LEASE_ID);
        var transaction = trackedLaterChunkTransaction(9);
        transaction.init(chunk(false, false, false, BASE_CHUNK_ID + 1));

        transaction.close();

        assertThat(tracker.transactionsToRollback()).hasSize(1);
        verifyNoInteractions(incompleteTransactionAvailability);
    }

    @Test
    void commitAndCloseWithoutTrackerOrCloseListener() {
        assertThatNoException().isThrownBy(() -> {
            try (var transaction = untrackedTransaction()) {
                transaction.init(chunk(true, false, false, BASE_CHUNK_ID));
                append(transaction, 5);
                transaction.commit();
            }
            try (var transaction = untrackedTransaction()) {
                transaction.init(chunk(false, true, true, BASE_CHUNK_ID + 1));
                transaction.commit();
            }
        });
    }

    private ChunkedTransaction untrackedTransaction() {
        return new ChunkedTransaction(
                CursorContext.NULL_CONTEXT,
                1,
                StoreCursors.NULL,
                Commitment.NO_COMMITMENT,
                TransactionIdGenerator.EMPTY);
    }

    private ChunkedTransaction trackedFirstChunkTransaction() {
        var transaction = new ChunkedTransaction(
                CursorContext.NULL_CONTEXT,
                1,
                NO_METADATA,
                StoreCursors.NULL,
                Commitment.NO_COMMITMENT,
                TransactionIdGenerator.EXTERNAL_ID,
                tracker,
                incompleteTransactionAvailability);
        // the appender assigns the transaction id before the batch is appended
        transaction.transactionId(TX_ID);
        return transaction;
    }

    private ChunkedTransaction trackedLaterChunkTransaction(long lastBatchAppendIndex) {
        return trackedLaterChunkTransaction(lastBatchAppendIndex, Commitment.NO_COMMITMENT);
    }

    private ChunkedTransaction trackedLaterChunkTransaction(long lastBatchAppendIndex, Commitment commitment) {
        return new ChunkedTransaction(
                TX_ID,
                lastBatchAppendIndex,
                1,
                NO_METADATA,
                CursorContext.NULL_CONTEXT,
                StoreCursors.NULL,
                commitment,
                tracker,
                incompleteTransactionAvailability);
    }

    private static void append(ChunkedTransaction transaction, long appendIndex) {
        transaction.batchAppended(appendIndex, new LogPosition(0, appendIndex), new LogPosition(0, appendIndex + 1), 0);
    }

    private static ChunkedCommandBatch chunk(boolean first, boolean last, boolean rollback, long chunkId) {
        return new ChunkedCommandBatch(
                List.of(),
                new ChunkMetadata(
                        first,
                        last,
                        rollback,
                        UNKNOWN_APPEND_INDEX,
                        chunkId,
                        new MutableLong(UNKNOWN_CONSENSUS_INDEX),
                        new MutableLong(UNKNOWN_APPEND_INDEX),
                        1,
                        UNKNOWN_TX_ID,
                        CHUNK_COMMIT_TIME,
                        LEASE_ID,
                        LATEST_KERNEL_VERSION,
                        AUTH_DISABLED));
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
