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
package org.neo4j.kernel.recovery;

import static org.neo4j.kernel.recovery.TransactionStatus.INCOMPLETE;
import static org.neo4j.kernel.recovery.TransactionStatus.RECOVERABLE;
import static org.neo4j.kernel.recovery.TransactionStatus.ROLLED_BACK;

import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.storageengine.api.CommandBatch;

public class TransactionIdTracker {

    private final MutableLongSet completedTransactionsWindow = LongSets.mutable.empty();
    private final MutableLongSet rollbackTransactions = LongSets.mutable.empty();
    private final MutableLongSet notCompletedTransactions = LongSets.mutable.empty();

    TransactionStatus transactionStatus(long transactionId) {
        if (notCompletedTransactions.contains(transactionId)) {
            return INCOMPLETE;
        }
        if (rollbackTransactions.contains(transactionId)) {
            return ROLLED_BACK;
        }
        return RECOVERABLE;
    }

    long[] notCompletedTransactions() {
        return notCompletedTransactions.toSortedArray();
    }

    public void trackBatch(CommittedCommandBatchRepresentation committedBatch) {
        CommandBatch commandBatch = committedBatch.commandBatch();
        if (commandBatch.isFirst() && commandBatch.isLast()) {
            return;
        }
        long transactionId = committedBatch.txId();

        if (commandBatch.isLast()) {
            completedTransactionsWindow.add(transactionId);
            if (committedBatch.isRollback()) {
                rollbackTransactions.add(transactionId);
            }
        } else {
            if (!completedTransactionsWindow.contains(transactionId)) {
                // we encountered transaction that we never completed, so we will need to rollback it
                notCompletedTransactions.add(transactionId);
            }
            // we are not really interested in keeping the whole set; window of this transaction is gone now
            // so, we can stop tracking it now
            if (commandBatch.isFirst()) {
                completedTransactionsWindow.remove(transactionId);
            }
        }
    }
}
