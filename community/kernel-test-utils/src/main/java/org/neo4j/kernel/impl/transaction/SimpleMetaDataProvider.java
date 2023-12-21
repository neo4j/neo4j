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

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;

public class SimpleMetaDataProvider implements MetadataProvider {
    private final SimpleTransactionIdStore transactionIdStore;
    private final SimpleLogVersionRepository logVersionRepository;
    private final ExternalStoreId externalStoreId = new ExternalStoreId(UUID.randomUUID());

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
    public long committingTransactionId() {
        return transactionIdStore.committingTransactionId();
    }

    @Override
    public void transactionCommitted(long transactionId, int checksum, long commitTimestamp, long consensusIndex) {
        transactionIdStore.transactionCommitted(transactionId, checksum, commitTimestamp, consensusIndex);
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
    public void setLastCommittedAndClosedTransactionId(
            long transactionId,
            int checksum,
            long commitTimestamp,
            long consensusIndex,
            long byteOffset,
            long logVersion) {
        transactionIdStore.setLastCommittedAndClosedTransactionId(
                transactionId, checksum, commitTimestamp, consensusIndex, byteOffset, logVersion);
    }

    @Override
    public void transactionClosed(
            long transactionId,
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        transactionIdStore.transactionClosed(
                transactionId, logVersion, byteOffset, checksum, commitTimestamp, consensusIndex);
    }

    @Override
    public void resetLastClosedTransaction(
            long transactionId,
            long logVersion,
            long byteOffset,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        transactionIdStore.resetLastClosedTransaction(
                transactionId, byteOffset, logVersion, checksum, commitTimestamp, consensusIndex);
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
}
