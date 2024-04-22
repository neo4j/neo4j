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
package org.neo4j.storageengine.util;

import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.TransactionId;

/**
 * Can accept offerings about {@link TransactionId}, but will always only keep the highest one,
 * always available in {@link #get()}.
 */
public class HighestTransactionId {
    private final AtomicReference<TransactionId> highest = new AtomicReference<>();

    public HighestTransactionId(TransactionId transactionId) {
        highest.set(transactionId);
    }

    /**
     * Offers a transaction id. Will be accepted if this is higher than the current highest.
     * This method is thread-safe.
     *
     * @param transactionId transaction id to compare for highest.
     * @param appendIndex transaction append index
     * @param kernelVersion transaction kernel version.
     * @param checksum checksum of the transaction.
     * @param commitTimestamp commit time for transaction with {@code transaction}.
     * @param consensusIndex consensus index for transaction with {@code transaction}.
     * @return {@code true} if the given transaction id was higher than the current highest,
     * {@code false}.
     */
    public boolean offer(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        TransactionId high = highest.get();
        if (transactionId < high.id()) { // a higher id has already been offered
            return false;
        }

        TransactionId update =
                new TransactionId(transactionId, appendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex);
        while (!highest.compareAndSet(high, update)) {
            high = highest.get();
            if (high.id()
                    >= transactionId) { // apparently someone else set a higher id while we were trying to set this id
                return false;
            }
        }
        // we set our id as the highest
        return true;
    }

    /**
     * Overrides the highest transaction id value, no matter what it currently is. Used for initialization purposes.
     *
     * @param transactionId id of the transaction.
     * @param appendIndex transaction append index
     * @param kernelVersion transaction kernel version.
     * @param checksum checksum of the transaction.
     * @param commitTimestamp commit time for transaction with {@code transaction}.
     * @param consensusIndex consensus index for transaction with {@code transaction}.
     */
    public final void set(
            long transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long commitTimestamp,
            long consensusIndex) {
        highest.set(new TransactionId(
                transactionId, appendIndex, kernelVersion, checksum, commitTimestamp, consensusIndex));
    }

    /**
     * @return the currently highest transaction together with its checksum.
     */
    public TransactionId get() {
        return highest.get();
    }
}
