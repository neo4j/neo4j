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

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.StorageEngineTransaction;

class QueueTransactionAppender extends LifecycleAdapter implements TransactionAppender {
    private final TransactionLogQueue transactionLogQueue;

    QueueTransactionAppender(TransactionLogQueue transactionLogQueue) {
        this.transactionLogQueue = transactionLogQueue;
    }

    @Override
    public void start() throws Exception {
        transactionLogQueue.start();
    }

    @Override
    public void shutdown() throws Exception {
        transactionLogQueue.shutdown();
    }

    @Override
    public long append(StorageEngineTransaction batch, LogAppendEvent logAppendEvent)
            throws IOException, ExecutionException, InterruptedException {
        long committedTxId = transactionLogQueue.submit(batch, logAppendEvent).getCommittedTxId();
        publishAsCommitted(batch);
        return committedTxId;
    }

    private static void publishAsCommitted(StorageEngineTransaction batch) {
        while (batch != null) {
            batch.commit();
            batch = batch.next();
        }
    }
}
