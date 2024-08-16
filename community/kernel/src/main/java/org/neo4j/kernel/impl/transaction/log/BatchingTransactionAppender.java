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

import static org.neo4j.kernel.KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;

import java.io.IOException;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.AppendTransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Panic;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.TransactionIdStore;

/**
 * Concurrently appends transactions to the transaction log, while coordinating with the log rotation and forcing the
 * log file in batches for higher throughput in a concurrent scenario.
 */
class BatchingTransactionAppender extends LifecycleAdapter implements TransactionAppender {
    private final LogFile logFile;
    private final AppendIndexProvider appendIndexProvider;
    private final TransactionMetadataCache metadataCache;
    private final LogRotation logRotation;
    private final Panic databasePanic;

    private TransactionLogWriter transactionLogWriter;
    private int previousChecksum;

    BatchingTransactionAppender(
            LogFiles logFiles,
            TransactionIdStore transactionIdStore,
            Panic databasePanic,
            AppendIndexProvider appendIndexProvider,
            TransactionMetadataCache metadataCache) {
        this.logFile = logFiles.getLogFile();
        this.appendIndexProvider = appendIndexProvider;
        this.metadataCache = metadataCache;
        this.logRotation = logFile.getLogRotation();
        this.databasePanic = databasePanic;
        this.previousChecksum = transactionIdStore.getLastCommittedTransaction().checksum();
    }

    @Override
    public void start() {
        this.transactionLogWriter = logFile.getTransactionLogWriter();
    }

    @Override
    public long append(StorageEngineTransaction batch, LogAppendEvent logAppendEvent) throws IOException {
        // Assigned base tx id just to make compiler happy
        long lastAppendIndex = BASE_APPEND_INDEX;
        // Synchronized with logFile to get absolute control over concurrent rotations happening
        synchronized (logFile) {
            // Assert that kernel is healthy before making any changes
            databasePanic.assertNoPanic(IOException.class);
            try (AppendTransactionEvent appendEvent = logAppendEvent.beginAppendTransaction(1)) {
                // Append all transactions in this batch to the log under the same logFile monitor
                StorageEngineTransaction commands = batch;
                while (commands != null) {
                    long appendIndex = appendIndexProvider.nextAppendIndex();
                    appendToLog(commands, appendIndex, logAppendEvent);
                    commands = commands.next();
                    lastAppendIndex = appendIndex;
                }
            }
        }

        // At this point we've appended all transactions in this batch, but we can't mark any of them
        // as committed since they haven't been forced to disk yet. So here we force, or potentially
        // piggy-back on another force, but anyway after this call below we can be sure that all our transactions
        // in this batch exist durably on disk.
        if (logFile.forceAfterAppend(logAppendEvent)) {
            // We got lucky and were the one forcing the log. It's enough if ones of all doing concurrent committers
            // checks the need for log rotation.
            if (checkIfRotationCheckIsRequired(batch)) {
                logAppendEvent.setLogRotated(logRotation.rotateLogIfNeeded(logAppendEvent));
            }
        }

        // Mark all transactions as committed
        publishAsCommitted(batch);

        return lastAppendIndex;
    }

    private static boolean checkIfRotationCheckIsRequired(StorageEngineTransaction batch) {
        // for envelopes, rotation happens within the channel during appends so rotating post-append isn't required
        return batch != null
                && batch.commandBatch().kernelVersion().isLessThan(VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED);
    }

    private static void publishAsCommitted(StorageEngineTransaction batch) {
        while (batch != null) {
            batch.commit();
            batch = batch.next();
        }
    }

    private void appendToLog(StorageEngineTransaction commands, long appendIndex, LogAppendEvent logAppendEvent)
            throws IOException {
        // The outcome of this try block is either of:
        // a) transaction successfully appended, at which point we return a Commitment to be used after force
        // b) transaction failed to be appended, at which point a kernel panic is issued
        // The reason that we issue a kernel panic on failure in here is that at this point we're still
        // holding the logFile monitor, and a failure to append needs to be communicated with potential
        // log rotation, which will wait for all transactions closed or fail on kernel panic.
        try {
            transactionLogWriter.resetAppendedBytesCounter();
            this.previousChecksum = transactionLogWriter.append(
                    commands.commandBatch(),
                    commands.transactionId(),
                    commands.chunkId(),
                    appendIndex,
                    previousChecksum,
                    commands.previousBatchAppendIndex(),
                    logAppendEvent);

            var logPositionBeforeCommit = transactionLogWriter.beforeAppendPosition();
            metadataCache.cacheTransactionMetadata(appendIndex, logPositionBeforeCommit);
            var logPositionAfterCommit = transactionLogWriter.getCurrentPosition();
            logAppendEvent.appendedBytes(transactionLogWriter.getAppendedBytes());
            commands.batchAppended(appendIndex, logPositionBeforeCommit, logPositionAfterCommit, previousChecksum);
        } catch (final Throwable panic) {
            databasePanic.panic(panic);
            throw panic;
        }
    }
}
