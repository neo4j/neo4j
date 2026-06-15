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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Panic;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.LogMetadataProviderImpl;
import org.neo4j.storageengine.api.LogPositionMetadata;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class PreFlushedTransactionAppenderTest {
    private final Panic databasePanic = mock(DatabaseHealth.class);
    private final TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
    private final TransactionIdGenerator transactionIdGenerator = TransactionIdGenerator.EXTERNAL_ID;
    private static final long FIRST_APPEND_INDEX = BASE_APPEND_INDEX + 1;

    @Test
    void shouldRegisterCommitmentsForBatchOfTransactions()
            throws IOException, ExecutionException, InterruptedException {
        // GIVEN - a publisher, and a complete transaction batch
        var appendIndexProvider = new LogMetadataProviderImpl(new EmptyLogTailMetadata(Config.defaults()));
        var metadataCache = new TransactionMetadataCache();
        var publisher = new PreFlushedTransactionAppender(databasePanic, appendIndexProvider, metadataCache);
        when(transactionIdStore.nextCommittingTransactionId())
                .thenReturn(FIRST_APPEND_INDEX, FIRST_APPEND_INDEX + 1, FIRST_APPEND_INDEX + 2);

        CompleteTransaction batch = completeTxBatch(3, PreFlushedTransactionAppenderTest::createBelievableMetadata);

        // WHEN - we publish
        long lastAppendIndex = publisher.register(batch, LogAppendEvent.NULL);

        // THEN - it returns the correct append index
        assertEquals(4, lastAppendIndex);

        LogPositionMetadata lastExpected = createBelievableMetadata(FIRST_APPEND_INDEX + 2);
        for (long appendIndex = FIRST_APPEND_INDEX; appendIndex < FIRST_APPEND_INDEX + 3; appendIndex++) {
            LogPositionMetadata expected = createBelievableMetadata(appendIndex);

            // THEN - it has written and published the commitments from a batch of transactions
            verify(transactionIdStore)
                    .transactionCommitted(
                            eq(appendIndex),
                            eq(appendIndex),
                            eq(LATEST_KERNEL_VERSION),
                            eq(expected.checksum()),
                            eq(0L),
                            eq(0L));

            // THEN - The metadata cache is furnished with the correct LogPositions
            var txMetadata = metadataCache.getTransactionMetadata(appendIndex);
            assertEquals(expected.prePosition(), txMetadata.startPosition());
        }
    }

    @Test
    void shouldThrowIfMetadataNotProperlySet() {
        // GIVEN - a publisher, and a complete transaction batch with invalid metadata
        var appendIndexProvider = new LogMetadataProviderImpl(new EmptyLogTailMetadata(Config.defaults()));
        var metadataCache = new TransactionMetadataCache();
        var publisher = new PreFlushedTransactionAppender(databasePanic, appendIndexProvider, metadataCache);

        CompleteTransaction batch = completeTxBatch(1, LogPositionMetadata::metadataWithJustAppendIndex);

        // WHEN - we publish
        // THEN - it's illegal
        assertThatThrownBy(() -> publisher.register(batch, LogAppendEvent.NULL))
                .hasMessageContaining("Attempted to publish transaction with invalid LogPositionMetadata: ");

        // THEN - nothing was published
        verify(transactionIdStore, never())
                .transactionCommitted(anyLong(), anyLong(), any(), anyInt(), anyLong(), anyLong());

        // THEN - no metadata was ever cached
        assertNull(metadataCache.getTransactionMetadata(FIRST_APPEND_INDEX));
    }

    /**
     * Create a batch of {@link CompleteTransaction CompleteTransactions} with valid {@link LogPositionMetadata}.
     *
     * @param transactions number of txs to make.
     * @return the first tx in the batch.
     */
    private CompleteTransaction completeTxBatch(
            int transactions, Function<Long, LogPositionMetadata> metadataFunction) {
        CompleteTransaction first = null;
        CompleteTransaction last = null;
        var transactionCommitment = new TransactionCommitment(transactionIdStore);
        for (long appendIndex = FIRST_APPEND_INDEX; appendIndex < FIRST_APPEND_INDEX + transactions; appendIndex++) {
            CompleteTransaction tx = new CompleteTransaction(
                    transaction(appendIndex),
                    NULL_CONTEXT,
                    StoreCursors.NULL,
                    transactionCommitment,
                    transactionIdGenerator,
                    metadataFunction.apply(appendIndex));
            if (first == null) {
                first = last = tx;
            } else {
                last.next(tx);
                last = tx;
            }
        }
        return first;
    }

    private static CommandBatch transaction(long appendIndex) {
        CompleteCommandBatch storageCommands = new CompleteCommandBatch(
                Collections.singletonList(new TestCommand()),
                0,
                0,
                1,
                0,
                -1,
                Leases.NO_LEASES,
                LATEST_KERNEL_VERSION,
                ANONYMOUS);
        storageCommands.setAppendIndex(appendIndex);
        return storageCommands;
    }

    private static LogPositionMetadata createBelievableMetadata(long appendIndex) {
        // These values need to be unique between metadata for comparison, but not necessarily true to life.
        return new LogPositionMetadata(
                appendIndex,
                new LogPosition(INITIAL_LOG_VERSION, appendIndex * 64),
                new LogPosition(INITIAL_LOG_VERSION, (appendIndex + 1) * 64 - 1),
                (int) (-appendIndex));
    }
}
