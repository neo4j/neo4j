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

import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.kernel.KernelVersion;

public class ChunkedTransactionTracker {
    private final ConcurrentHashMap<Long, TransactionInfo> registry = new ConcurrentHashMap<>();

    public Collection<TransactionInfo> transactionsToRollback() {
        return registry.values();
    }

    public long firstBatchAppendIndex(long transactionId) {
        var firstBatchAppendIndex = registry.get(transactionId).firstBatchAppendIndex;
        assert firstBatchAppendIndex != UNKNOWN_APPEND_INDEX : "First batch append index isn't set properly.";
        return firstBatchAppendIndex;
    }

    public void registerChunkedTransaction(
            long transactionId,
            long firstBatchAppendIndex,
            long lastBatchAppendIndex,
            long chunkId,
            KernelVersion kernelVersion,
            long leaseId) {
        TransactionInfo txInfo = new TransactionInfo(
                transactionId, firstBatchAppendIndex, lastBatchAppendIndex, chunkId, kernelVersion, leaseId);
        registry.put(transactionId, txInfo);
    }

    public void cleanupChunkedTransaction(long transactionId) {
        registry.remove(transactionId);
    }

    public void clear() {
        registry.clear();
    }

    public record TransactionInfo(
            long transactionId,
            long firstBatchAppendIndex,
            long lastBatchAppendIndex,
            long chunkId,
            KernelVersion kernelVersion,
            long leaseId) {}
}
