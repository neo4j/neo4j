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
package org.neo4j.kernel.recovery;

import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.storageengine.api.CommandBatch;

public class TransactionIdTracker {

    private final MutableLongSet completedTransactions = LongSets.mutable.empty();
    private final MutableLongSet notCompletedTransactions = LongSets.mutable.empty();

    boolean isCompletedTransaction(long transactionId) {
        return !notCompletedTransactions.contains(transactionId);
    }

    public void trackBatch(CommittedCommandBatch committedBatch) {
        CommandBatch commandBatch = committedBatch.commandBatch();
        if (commandBatch.isFirst() && commandBatch.isLast()) {
            return;
        }
        long transactionId = committedBatch.txId();
        if (commandBatch.isLast()) {
            completedTransactions.add(transactionId);
        } else {

            if (!completedTransactions.contains(transactionId)) {
                notCompletedTransactions.add(transactionId);
            }
            // if this is first batch be can remove it's from tacking now as well
            if (commandBatch.isFirst()) {
                completedTransactions.remove(transactionId);
            }
        }
    }
}
