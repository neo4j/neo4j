/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.context;

import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import org.neo4j.io.pagecache.context.OldestTransactionIdFactory;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.io.pagecache.context.TransactionIdSnapshotFactory;
import org.neo4j.io.pagecache.context.VersionContext;

/**
 * Transactional version context that used by read transaction to read data of specific version.
 * Or perform versioned data modification.
 */
public class TransactionVersionContext implements VersionContext {
    private static final long UNKNOWN_OBSOLETE_HEAD_VERSION = -1;
    private final TransactionIdSnapshotFactory transactionIdSnapshotFactory;
    private final OldestTransactionIdFactory oldestTransactionIdFactory;
    private long transactionId = BASE_TX_ID - 1;
    private TransactionIdSnapshot transactionIds;
    private long oldestTransactionId = BASE_TX_ID - 1;
    private long updatedChainHeadVersion;
    private boolean dirty;

    public TransactionVersionContext(
            TransactionIdSnapshotFactory transactionIdSnapshotFactory, OldestTransactionIdFactory oldestIdFactory) {
        this.transactionIdSnapshotFactory = transactionIdSnapshotFactory;
        this.oldestTransactionIdFactory = oldestIdFactory;
    }

    @Override
    public void initRead() {
        refreshVisibilityBoundaries();
        dirty = false;
    }

    @Override
    public void initWrite(long committingTxId) {
        assert committingTxId >= BASE_TX_ID;
        transactionId = committingTxId;
        oldestTransactionId = oldestTransactionIdFactory.oldestTransactionId();
    }

    @Override
    public long committingTransactionId() {
        return transactionId;
    }

    @Override
    public long lastClosedTransactionId() {
        return transactionIds.lastClosedTxId();
    }

    @Override
    public long highestClosed() {
        return transactionIds.highestEverSeen();
    }

    @Override
    public long[] notVisibleTransactionIds() {
        return transactionIds.notVisibleTransactions();
    }

    @Override
    public long oldestVisibleTransactionNumber() {
        return oldestTransactionId;
    }

    @Override
    public void refreshVisibilityBoundaries() {
        transactionIds = transactionIdSnapshotFactory.createSnapshot();
    }

    @Override
    public void invisibleChainHead(long headVersion) {
        updatedChainHeadVersion = headVersion;
    }

    @Override
    public boolean obsoleteHeadObserved() {
        return updatedChainHeadVersion > 0;
    }

    @Override
    public void resetObsoleteHeadState() {
        updatedChainHeadVersion = UNKNOWN_OBSOLETE_HEAD_VERSION;
    }

    @Override
    public long currentInvisibleChainHeadVersion() {
        return updatedChainHeadVersion;
    }

    @Override
    public void markAsDirty() {
        dirty = true;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public boolean initializedForWrite() {
        return transactionId >= BASE_TX_ID;
    }
}
