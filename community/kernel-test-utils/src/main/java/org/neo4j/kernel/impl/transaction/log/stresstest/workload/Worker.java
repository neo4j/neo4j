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
package org.neo4j.kernel.impl.transaction.log.stresstest.workload;

import java.util.function.BooleanSupplier;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.storageengine.api.TransactionIdStore;

class Worker implements Runnable {
    private final TransactionAppender transactionAppender;
    private final TransactionRepresentationFactory factory;
    private final BooleanSupplier condition;

    Worker(
            TransactionAppender transactionAppender,
            TransactionRepresentationFactory factory,
            BooleanSupplier condition) {
        this.transactionAppender = transactionAppender;
        this.factory = factory;
        this.condition = condition;
    }

    @Override
    public void run() {
        long latestTxId = TransactionIdStore.BASE_TX_ID;
        while (condition.getAsBoolean()) {
            CompleteTransaction transaction = factory.nextTransaction(latestTxId);
            try {
                latestTxId = transactionAppender.append(transaction, LogAppendEvent.NULL);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
