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
package org.neo4j.kernel.impl.transaction.tracing;

import java.nio.file.Path;
import org.neo4j.io.pagecache.context.CursorContext;

public interface DatabaseTracer extends TransactionTracer, CheckPointTracer {
    DatabaseTracer NULL = new DatabaseTracer() {
        @Override
        public long numberOfCheckPoints() {
            return 0;
        }

        @Override
        public long checkPointAccumulatedTotalTimeMillis() {
            return 0;
        }

        @Override
        public long lastCheckpointTimeMillis() {
            return 0;
        }

        @Override
        public long lastCheckpointPagesFlushed() {
            return 0;
        }

        @Override
        public long lastCheckpointIOs() {
            return 0;
        }

        @Override
        public long lastCheckpointIOLimit() {
            return 0;
        }

        @Override
        public long lastCheckpointIOLimitedTimes() {
            return 0;
        }

        @Override
        public long lastCheckpointIOLimitedMillis() {
            return 0;
        }

        @Override
        public long flushedBytes() {
            return 0;
        }

        @Override
        public LogFileCreateEvent createLogFile() {
            return LogFileCreateEvent.NULL;
        }

        @Override
        public void openLogFile(Path filePath) {}

        @Override
        public void closeLogFile(Path filePath) {}

        @Override
        public LogAppendEvent logAppend() {
            return LogAppendEvent.NULL;
        }

        @Override
        public LogFileFlushEvent flushFile() {
            return LogFileFlushEvent.NULL;
        }

        @Override
        public LogCheckPointEvent beginCheckPoint() {
            return LogCheckPointEvent.NULL;
        }

        @Override
        public TransactionEvent beginTransaction(CursorContext cursorContext) {
            return TransactionEvent.NULL;
        }

        @Override
        public TransactionWriteEvent beginAsyncCommit() {
            return TransactionWriteEvent.NULL;
        }

        @Override
        public long appendedBytes() {
            return 0;
        }

        @Override
        public long numberOfLogRotations() {
            return 0;
        }

        @Override
        public long logRotationAccumulatedTotalTimeMillis() {
            return 0;
        }

        @Override
        public long lastLogRotationTimeMillis() {
            return 0;
        }

        @Override
        public long numberOfFlushes() {
            return 0;
        }

        @Override
        public long lastTransactionLogAppendBatch() {
            return 0;
        }

        @Override
        public long batchesAppended() {
            return 0;
        }

        @Override
        public long rolledbackBatches() {
            return 0;
        }

        @Override
        public long rolledbackBatchedTransactions() {
            return 0;
        }
    };

    LogFileCreateEvent createLogFile();

    void openLogFile(Path filePath);

    void closeLogFile(Path filePath);

    LogAppendEvent logAppend();

    LogFileFlushEvent flushFile();
}
