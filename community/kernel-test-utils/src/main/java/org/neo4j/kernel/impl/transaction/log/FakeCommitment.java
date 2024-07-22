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

import static org.neo4j.kernel.KernelVersion.DEFAULT_BOOTSTRAP_VERSION;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.Commitment;
import org.neo4j.storageengine.api.TransactionIdStore;

public class FakeCommitment implements Commitment {
    public static final int CHECKSUM = 3;
    public static final long TIMESTAMP = 8194639457389L;
    public static final long CONSENSUS_INDEX = 1456L;
    private final long id;
    private final long appendIndex;
    private final TransactionIdStore transactionIdStore;
    private boolean committed;

    public FakeCommitment(long id, long appendIndex, TransactionIdStore transactionIdStore, boolean markedAsCommitted) {
        this.id = id;
        this.appendIndex = appendIndex;
        this.transactionIdStore = transactionIdStore;
        this.committed = markedAsCommitted;
    }

    @Override
    public void commit(
            long transactionId,
            long appendIndex,
            boolean firstBatch,
            boolean lastBatch,
            KernelVersion kernelVersion,
            LogPosition logPositionBeforeCommit,
            LogPosition logPositionAfterCommit,
            int checksum,
            long consensusIndex) {}

    @Override
    public void publishAsCommitedLastBatch() {}

    @Override
    public void publishAsCommitted(long transactionCommitTimestamp, long appendIndex) {
        committed = true;
        transactionIdStore.transactionCommitted(
                id, appendIndex, DEFAULT_BOOTSTRAP_VERSION, CHECKSUM, TIMESTAMP, CONSENSUS_INDEX);
    }

    @Override
    public void publishAsClosed() {
        if (committed) {
            transactionIdStore.transactionClosed(
                    id, appendIndex, DEFAULT_BOOTSTRAP_VERSION, 1, 2, CHECKSUM, TIMESTAMP, CONSENSUS_INDEX);
        }
    }
}
