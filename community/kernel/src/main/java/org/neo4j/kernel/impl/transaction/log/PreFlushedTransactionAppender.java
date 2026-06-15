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

import static java.lang.String.format;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Panic;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.LogPositionMetadata;
import org.neo4j.storageengine.api.StorageEngineTransaction;

/**
 * Registers transactions as having been committed, without appending them to any log. This implementation is designed
 * for a context where a single log holds all transaction and Raft content, and appending to that log has already
 * happened by the time {@link #register(StorageEngineTransaction, LogAppendEvent) register} is called.
 */
public class PreFlushedTransactionAppender extends LifecycleAdapter implements TransactionAppender {
    private final Panic databasePanic;
    private final AppendIndexProvider appendIndexProvider;
    private final TransactionMetadataCache metadataCache;

    public PreFlushedTransactionAppender(
            Panic databasePanic, AppendIndexProvider appendIndexProvider, TransactionMetadataCache metadataCache) {
        this.databasePanic = databasePanic;
        this.appendIndexProvider = appendIndexProvider;
        this.metadataCache = metadataCache;
    }

    @Override
    public long register(StorageEngineTransaction batch, LogAppendEvent logAppendEvent)
            throws IOException, ExecutionException, InterruptedException {
        // Assigned base tx id just to make compiler happy
        long lastAppendIndex = BASE_APPEND_INDEX;
        // Assert that kernel is healthy before making any changes
        databasePanic.assertNoPanic(IOException.class);
        try (AppendTransactionEvent appendEvent = logAppendEvent.beginAppendTransaction(1)) {
            // Publish and cache all transactions
            StorageEngineTransaction commands = batch;
            while (commands != null) {
                LogPositionMetadata metadata = commands.logPositionMetadata();

                // Compute txid
                if (commands.commandBatch().isFirst()) {
                    commands.transactionId(metadata.appendIndex());
                } else {
                    commands.transactionId();
                }

                if (!metadata.hasValidPositionData()) {
                    throw new IllegalArgumentException(
                            format("Attempted to publish transaction with invalid LogPositionMetadata: %s", metadata));
                }

                if (metadata.appendIndex() != commands.commandBatch().appendIndex()) {
                    throw new IllegalStateException(format(
                            "Expected append index, both on the metadata (%d) and on the command batch (%d), to be "
                                    + "equal, but they aren't. Transaction in question: %s, which starts at position: "
                                    + "%s",
                            metadata.appendIndex(),
                            commands.commandBatch().appendIndex(),
                            commands,
                            metadata.prePosition()));
                }

                if (commands.commandBatch().isFirst() && commands.transactionId() != metadata.appendIndex()) {
                    throw new IllegalStateException(format(
                            "Expected transactionId (%d) and append index (%d), to be equal, but they aren't. "
                                    + "Transaction in question: %s, which starts at position: %s",
                            commands.transactionId(), metadata.appendIndex(), commands, metadata.prePosition()));
                }

                // Maintain and validate append index
                long nextProvidedAppendIndex = appendIndexProvider.nextAppendIndex();
                if (nextProvidedAppendIndex != metadata.appendIndex()) {
                    throw new IllegalStateException(format(
                            "Append index (%d) was supposed to be %d. The fact that it isn't means that append index "
                                    + "isn't monotonically increasing. Transaction in question: %s, which starts at "
                                    + "position: %s",
                            metadata.appendIndex(), nextProvidedAppendIndex, commands, metadata.prePosition()));
                }

                // Because we're on Merged Logs, some txs (specifically non-first multichunked) need to take an extra
                // step to maintain our gap-free sequences properly.
                commands.fillGapsOnCloseIfRelevant(true);
                // Inform TransactionMetadataCache and TransactionIdStore of new tx
                metadataCache.cacheTransactionMetadata(metadata.appendIndex(), metadata.prePosition());
                commands.batchAppended(
                        metadata.appendIndex(), metadata.prePosition(), metadata.postPosition(), metadata.checksum());
                // Mark as committed
                commands.commit();

                commands = commands.next();
                lastAppendIndex = metadata.appendIndex();
            }
        }

        return lastAppendIndex;
    }
}
