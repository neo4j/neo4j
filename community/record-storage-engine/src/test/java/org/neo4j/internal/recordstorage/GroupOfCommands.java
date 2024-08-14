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
package org.neo4j.internal.recordstorage;

import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.util.List;
import java.util.function.LongConsumer;
import org.neo4j.common.Subject;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;

public class GroupOfCommands implements StorageEngineTransaction {
    private final long transactionId;
    private final StoreCursors storeCursors;
    private final StorageCommand[] commands;
    StorageEngineTransaction next;

    public GroupOfCommands(StoreCursors storeCursors, StorageCommand... commands) {
        this(TransactionIdStore.BASE_TX_ID, storeCursors, commands);
    }

    public GroupOfCommands(long transactionId, StoreCursors storeCursors, StorageCommand... commands) {
        this.transactionId = transactionId;
        this.storeCursors = storeCursors;
        this.commands = commands;
    }

    @Override
    public long transactionId() {
        return transactionId;
    }

    @Override
    public long chunkId() {
        return 0;
    }

    @Override
    public LogPosition previousBatchLogPosition() {
        return LogPosition.UNSPECIFIED;
    }

    @Override
    public Subject subject() {
        return Subject.SYSTEM;
    }

    @Override
    public CursorContext cursorContext() {
        return NULL_CONTEXT;
    }

    @Override
    public StoreCursors storeCursors() {
        return storeCursors;
    }

    @Override
    public StorageEngineTransaction next() {
        return next;
    }

    @Override
    public void next(StorageEngineTransaction next) {}

    @Override
    public void onClose(LongConsumer closedCallback) {}

    @Override
    public void commit() {}

    @Override
    public CommandBatch commandBatch() {
        return new CompleteCommandBatch(
                List.of(commands), 0, 0, 0, 0, 0, LatestVersions.LATEST_KERNEL_VERSION, Subject.SYSTEM);
    }

    @Override
    public void batchAppended(long appendIndex, LogPosition beforeCommit, LogPosition positionAfter, int checksum) {}

    @Override
    public void close() {}
}
