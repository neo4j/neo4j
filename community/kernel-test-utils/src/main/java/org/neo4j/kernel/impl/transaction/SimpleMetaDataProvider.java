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
package org.neo4j.kernel.impl.transaction;

import static org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata.EMPTY_APPEND_BATCH_INFO;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.AppendBatchInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.ClosedBatchMetadata;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.OpenTransactionMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.util.HighestAppendBatch;

public class SimpleMetaDataProvider implements MetadataProvider {
    private final SimpleTransactionIdStore transactionIdStore;
    private final SimpleLogVersionRepository logVersionRepository;
    private final ExternalStoreId externalStoreId = new ExternalStoreId(UUID.randomUUID());
    private final AtomicLong appendIndex = new AtomicLong();
    private final HighestAppendBatch appendBatchInfo = new HighestAppendBatch(EMPTY_APPEND_BATCH_INFO);

    public SimpleMetaDataProvider() {
        transactionIdStore = new SimpleTransactionIdStore();
        logVersionRepository = new SimpleLogVersionRepository();
    }

    @Override
    public void close() throws IOException {}

    @Override
    public long getCurrentLogVersion() {
        return logVersionRepository.getCurrentLogVersion();
    }

    @Override
    public void setCurrentLogVersion(long version) {
        logVersionRepository.setCurrentLogVersion(version);
    }

    @Override
    public long incrementAndGetVersion() {
        return logVersionRepository.incrementAndGetVersion();
    }

    @Override
    public long getCheckpointLogVersion() {
        return logVersionRepository.getCheckpointLogVersion();
    }

    @Override
    public void setCheckpointLogVersion(long version) {
        logVersionRepository.setCheckpointLogVersion(version);
    }

    @Override
    public long incrementAndGetCheckpointLogVersion() {
        return logVersionRepository.incrementAndGetCheckpointLogVersion();
    }

    @Override
    public StoreId getStoreId() {
        return new StoreId(1, 1, "engine-1", "format-1", 1, 1);
    }

    @Override
    public ExternalStoreId getExternalStoreId() {
        return externalStoreId;
    }

    @Override
    public long nextCommittingTransactionId() {
        return transactionIdStore.nextCommittingTransactionId();
    }

    @Override
    public void transactionCommitted(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        transactionIdStore.transactionCommitted(
                transactionId, appendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex);
    }

    @Override
    public long getLastCommittedTransactionId() {
        return transactionIdStore.getLastCommittedTransactionId();
    }

    @Override
    public TransactionId getLastCommittedTransaction() {
        return transactionIdStore.getLastCommittedTransaction();
    }

    @Override
    public long getLastClosedTransactionId() {
        return transactionIdStore.getLastClosedTransactionId();
    }

    @Override
    public TransactionIdSnapshot getClosedTransactionSnapshot() {
        return transactionIdStore.getClosedTransactionSnapshot();
    }

    @Override
    public ClosedTransactionMetadata getLastClosedTransaction() {
        return transactionIdStore.getLastClosedTransaction();
    }

    @Override
    public ClosedBatchMetadata getLastClosedBatch() {
        return transactionIdStore.getLastClosedBatch();
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
            long transactionId,
            long transactionAppendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex,
            long byteOffset,
            long logVersion,
            long appendIndex) {
        transactionIdStore.setLastCommittedAndClosedTransactionId(
                transactionId,
                transactionAppendIndex,
                kernelVersion,
                checksum,
                commitTimestamp,
                consensusIndex,
                byteOffset,
                logVersion,
                appendIndex);
        this.appendIndex.set(appendIndex);
        this.appendBatchInfo.set(appendIndex, LogPosition.UNSPECIFIED);
    }

    @Override
    public void transactionClosed(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        transactionIdStore.transactionClosed(
                transactionId,
                appendIndex,
                kernelVersion,
                logVersion,
                byteOffset,
                checksum,
                commitTimestamp,
                consensusIndex);
    }

    @Override
    public void batchClosed(
            long transactionId,
            long appendIndex,
            boolean firstBatch,
            boolean lastBatch,
            KernelVersion kernelVersion,
            LogPosition logPositionAfter) {
        transactionIdStore.batchClosed(
                transactionId, appendIndex, firstBatch, lastBatch, kernelVersion, logPositionAfter);
    }

    @Override
    public void resetLastClosedTransaction(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        transactionIdStore.resetLastClosedTransaction(
                transactionId,
                appendIndex,
                kernelVersion,
                byteOffset,
                logVersion,
                checksum,
                commitTimestamp,
                consensusIndex);
    }

    @Override
    public void appendBatch(
            long transactionId,
            long appendIndex,
            boolean firstBatch,
            boolean lastBatch,
            LogPosition logPositionBefore,
            LogPosition logPositionAfter) {
        appendBatchInfo.offer(appendIndex, logPositionAfter);
    }

    @Override
    public AppendBatchInfo getLastCommittedBatch() {
        return appendBatchInfo.get();
    }

    @Override
    public OpenTransactionMetadata getOldestOpenTransaction() {
        return transactionIdStore.getOldestOpenTransaction();
    }

    @Override
    public TransactionId getHighestEverClosedTransaction() {
        return transactionIdStore.getHighestEverClosedTransaction();
    }

    @Override
    public Optional<UUID> getDatabaseIdUuid(CursorContext cursorContext) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public void setDatabaseIdUuid(UUID uuid, CursorContext cursorContext) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public void regenerateMetadata(StoreId storeId, UUID externalStoreUUID, CursorContext cursorContext) {
        throw new UnsupportedOperationException("RegenerateMetadata is not supported.");
    }

    @Override
    public long nextAppendIndex() {
        return appendIndex.incrementAndGet();
    }

    @Override
    public long getLastAppendIndex() {
        return appendIndex.getAcquire();
    }
}
