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

import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionCommitFailed;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionLogError;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.OutOfDiskSpaceException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.storageengine.api.CommandBatchToApply;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;

public class InternalTransactionCommitProcess implements TransactionCommitProcess {
    private final TransactionAppender appender;
    private final StorageEngine storageEngine;
    private final boolean preAllocateSpaceInStores;
    private final CommandCommitListeners commandCommitListeners;

    public InternalTransactionCommitProcess(
            TransactionAppender appender,
            StorageEngine storageEngine,
            boolean preAllocateSpaceInStores,
            CommandCommitListeners commandCommitListeners) {
        this.appender = appender;
        this.storageEngine = storageEngine;
        this.preAllocateSpaceInStores = preAllocateSpaceInStores;
        this.commandCommitListeners = commandCommitListeners;
    }

    @Override
    public long commit(
            CommandBatchToApply batch, TransactionWriteEvent transactionWriteEvent, TransactionApplicationMode mode)
            throws TransactionFailureException {
        try {
            if (preAllocateSpaceInStores) {
                preAllocateSpaceInStores(batch, transactionWriteEvent, mode);
            }

            long lastTxId = appendToLog(batch, transactionWriteEvent);
            try {
                applyToStore(batch, transactionWriteEvent, mode);
            } finally {
                close(batch);
            }

            commandCommitListeners.registerSuccess(batch.commandBatch(), lastTxId);
            return lastTxId;
        } catch (Exception e) {
            commandCommitListeners.registerFailure(batch.commandBatch(), e);
            throw e;
        }
    }

    private long appendToLog(CommandBatchToApply batch, TransactionWriteEvent transactionWriteEvent)
            throws TransactionFailureException {
        try (LogAppendEvent logAppendEvent = transactionWriteEvent.beginLogAppend()) {
            return appender.append(batch, logAppendEvent);
        } catch (Throwable cause) {
            throw new TransactionFailureException(
                    TransactionLogError, cause, "Could not append transaction: " + batch + " to log.");
        }
    }

    protected void applyToStore(
            CommandBatchToApply batch, TransactionWriteEvent transactionWriteEvent, TransactionApplicationMode mode)
            throws TransactionFailureException {
        try (StoreApplyEvent storeApplyEvent = transactionWriteEvent.beginStoreApply()) {
            storageEngine.apply(batch, mode);
        } catch (Throwable cause) {
            throw new TransactionFailureException(
                    TransactionCommitFailed,
                    cause,
                    "Could not apply the transaction: " + batch + " to the store after written to log.");
        }
    }

    private void preAllocateSpaceInStores(
            CommandBatchToApply batch, TransactionWriteEvent transactionWriteEvent, TransactionApplicationMode mode)
            throws TransactionFailureException {
        // FIXME ODP - add function to commitEvent to be able to trace?
        try {
            storageEngine.preAllocateStoreFilesForCommands(batch, mode);
        } catch (OutOfDiskSpaceException oods) {
            throw new TransactionFailureException(
                    // FIXME ODP - add an out of disk space status when we are ready to expose this functionality
                    Status.General.UnknownError,
                    oods,
                    "Could not preallocate disk space for the transaction: " + batch);
        } catch (Throwable cause) {
            throw new TransactionFailureException(
                    TransactionCommitFailed, cause, "Could not preallocate disk space for the transaction: " + batch);
        }
    }

    private static void close(CommandBatchToApply batch) {
        while (batch != null) {
            batch.close();
            batch = batch.next();
        }
    }
}
