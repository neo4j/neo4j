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
package org.neo4j.kernel.impl.transaction.log.files;

import static org.neo4j.storageengine.api.TransactionIdStore.emptyVersionedTransaction;

import java.util.Optional;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.log.AppendBatchInfo;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LastAppendBatchInfoProvider;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.DetachedLogTailScanner;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;

public class LogTailInformation implements LogTailMetadata {
    public final CheckpointInfo lastCheckPoint;
    public final long firstAppendIndexAfterLastCheckPoint;
    public final boolean filesNotFound;
    public final long currentLogVersion;
    public final byte firstLogEntryVersionAfterCheckpoint;
    private final boolean recordAfterCheckpoint;
    private final StoreId storeId;
    private final KernelVersionProvider fallbackKernelVersionProvider;
    private final LastAppendBatchInfoProvider lastAppendBatchInfoProvider;
    private AppendBatchInfo lastBatchInfo;

    public LogTailInformation(
            boolean recordAfterCheckpoint,
            long firstAppendIndexAfterLastCheckPoint,
            boolean filesNotFound,
            long currentLogVersion,
            byte firstLogEntryVersionAfterCheckpoint,
            KernelVersionProvider fallbackKernelVersionProvider,
            LastAppendBatchInfoProvider lastAppendBatchInfoProvider) {
        this(
                null,
                recordAfterCheckpoint,
                firstAppendIndexAfterLastCheckPoint,
                filesNotFound,
                currentLogVersion,
                firstLogEntryVersionAfterCheckpoint,
                null,
                fallbackKernelVersionProvider,
                lastAppendBatchInfoProvider);
    }

    public LogTailInformation(
            CheckpointInfo lastCheckPoint,
            boolean recordAfterCheckpoint,
            long firstAppendIndexAfterLastCheckPoint,
            boolean filesNotFound,
            long currentLogVersion,
            byte firstLogEntryVersionAfterCheckpoint,
            StoreId storeId,
            KernelVersionProvider fallbackKernelVersionProvider,
            LastAppendBatchInfoProvider lastAppendBatchInfoProvider) {
        this.lastCheckPoint = lastCheckPoint;
        this.firstAppendIndexAfterLastCheckPoint = firstAppendIndexAfterLastCheckPoint;
        this.filesNotFound = filesNotFound;
        this.currentLogVersion = currentLogVersion;
        this.firstLogEntryVersionAfterCheckpoint = firstLogEntryVersionAfterCheckpoint;
        this.recordAfterCheckpoint = recordAfterCheckpoint;
        this.storeId = storeId;
        this.fallbackKernelVersionProvider = fallbackKernelVersionProvider;
        this.lastAppendBatchInfoProvider = lastAppendBatchInfoProvider;
    }

    public boolean logsAfterLastCheckpoint() {
        return recordAfterCheckpoint;
    }

    @Override
    public boolean logsMissing() {
        return lastCheckPoint == null && filesNotFound;
    }

    @Override
    public boolean hasUnreadableBytesInCheckpointLogs() {
        return lastCheckPoint != null
                && !lastCheckPoint
                        .channelPositionAfterCheckpoint()
                        .equals(lastCheckPoint.checkpointFilePostReadPosition());
    }

    @Override
    public boolean isRecoveryRequired() {
        return recordAfterCheckpoint || logsMissing() || hasUnreadableBytesInCheckpointLogs();
    }

    @Override
    public Optional<StoreId> getStoreId() {
        return Optional.ofNullable(storeId);
    }

    @Override
    public Optional<CheckpointInfo> getLastCheckPoint() {
        return Optional.ofNullable(lastCheckPoint);
    }

    @Override
    public String toString() {
        return "LogTailInformation{" + "lastCheckPoint=" + lastCheckPoint + ", firstAppendIndexAfterLastCheckPoint="
                + firstAppendIndexAfterLastCheckPoint + ", filesNotFound="
                + filesNotFound + ", currentLogVersion=" + currentLogVersion + ", firstLogEntryVersionAfterCheckpoint="
                + firstLogEntryVersionAfterCheckpoint
                + ", recordAfterCheckpoint=" + recordAfterCheckpoint + '}';
    }

    @Override
    public long getCheckpointLogVersion() {
        if (lastCheckPoint == null) {
            return EMPTY_LOG_TAIL.getCheckpointLogVersion();
        }
        return lastCheckPoint.channelPositionAfterCheckpoint().getLogVersion();
    }

    @Override
    public KernelVersion kernelVersion() {
        if (lastCheckPoint != null) {
            return lastCheckPoint.kernelVersion();
        }

        // No checkpoint, but we did find some transactions. Since a recovery will happen in this case we
        // can just say we are on the version we saw in the first transaction. If we are on a later version we
        // will run into the upgrade transaction which will update this
        if (firstLogEntryVersionAfterCheckpoint != DetachedLogTailScanner.NO_ENTRY) {
            return KernelVersion.getForVersion(firstLogEntryVersionAfterCheckpoint);
        }

        // There was no checkpoint since it is the first start, or we restart after logs removal,
        // use the version specified as the fallback
        return fallbackKernelVersionProvider.kernelVersion();
    }

    @Override
    public long getLogVersion() {
        return filesNotFound ? EMPTY_LOG_TAIL.getLogVersion() : currentLogVersion;
    }

    @Override
    public TransactionId getLastCommittedTransaction() {
        if (lastCheckPoint == null) {
            return emptyVersionedTransaction(kernelVersion());
        }
        return lastCheckPoint.transactionId();
    }

    @Override
    public LogPosition getLastTransactionLogPosition() {
        if (lastCheckPoint == null) {
            return EMPTY_LOG_TAIL.getLastTransactionLogPosition();
        }
        return lastCheckPoint.transactionLogPosition();
    }

    @Override
    public long getLastCheckpointedAppendIndex() {
        if (lastCheckPoint == null) {
            return EMPTY_LOG_TAIL.getLastCheckpointedAppendIndex();
        }
        return lastCheckPoint.appendIndex();
    }

    @Override
    public AppendBatchInfo lastBatch() {
        if (filesNotFound) {
            return EMPTY_LOG_TAIL.lastBatch();
        }
        if (lastBatchInfo != null) {
            return lastBatchInfo;
        }
        lastBatchInfo = lastAppendBatchInfoProvider.get();
        return lastBatchInfo;
    }
}
