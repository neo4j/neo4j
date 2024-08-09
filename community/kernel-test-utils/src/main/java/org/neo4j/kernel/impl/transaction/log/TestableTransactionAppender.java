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

import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;

import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.StorageEngineTransaction;

public class TestableTransactionAppender extends LifecycleAdapter implements TransactionAppender {

    @Override
    public long append(StorageEngineTransaction batch, LogAppendEvent logAppendEvent) {
        long appendIndex = BASE_APPEND_INDEX;
        while (batch != null) {
            // force transaction id generation
            batch.transactionId();
            appendIndex = batch.commandBatch().appendIndex();
            batch.batchAppended(appendIndex, new LogPosition(appendIndex, 128), new LogPosition(appendIndex, 256), 1);
            batch.commit();
            batch = batch.next();
        }
        return appendIndex;
    }
}
