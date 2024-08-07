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
import java.util.ArrayList;
import java.util.List;
import org.neo4j.kernel.impl.transaction.ChunkedBatchRepresentation;
import org.neo4j.kernel.impl.transaction.ChunkedRollbackBatchRepresentation;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.CompleteBatchRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkEnd;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkStart;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryRollback;
import org.neo4j.storageengine.api.StorageCommand;

public class CommittedCommandBatchCursor implements CommandBatchCursor {
    private final ReadableLogPositionAwareChannel channel;
    private final LogEntryCursor logEntryCursor;
    private final LogPositionMarker lastGoodPositionMarker = new LogPositionMarker();

    private CommittedCommandBatchRepresentation current;

    public CommittedCommandBatchCursor(ReadableLogPositionAwareChannel channel, LogEntryReader entryReader)
            throws IOException {
        this.channel = channel;
        channel.getCurrentLogPosition(lastGoodPositionMarker);
        this.logEntryCursor = new LogEntryCursor(entryReader, channel);
    }

    @Override
    public CommittedCommandBatchRepresentation get() {
        return current;
    }

    @Override
    public boolean next() throws IOException {
        current = null;

        if (!logEntryCursor.next()) {
            return false;
        }

        LogEntry entry = logEntryCursor.get();
        List<StorageCommand> entries = new ArrayList<>();
        if (entry instanceof LogEntryRollback rollback) {
            current = new ChunkedRollbackBatchRepresentation(
                    rollback.kernelVersion(),
                    rollback.getTransactionId(),
                    rollback.getAppendIndex(),
                    rollback.getTimeWritten(),
                    rollback.getChecksum());
        } else if (entry instanceof LogEntryStart || entry instanceof LogEntryChunkStart) {
            LogEntry startEntry = entry;
            LogEntry endEntry;
            while (true) {
                if (!logEntryCursor.next()) {
                    return false;
                }

                entry = logEntryCursor.get();
                if (entry instanceof LogEntryCommit || entry instanceof LogEntryChunkEnd) {
                    endEntry = entry;
                    break;
                }

                LogEntryCommand command = (LogEntryCommand) entry;
                entries.add(command.getCommand());
            }
            if (startEntry instanceof LogEntryStart entryStart && endEntry instanceof LogEntryCommit commitEntry) {
                current = new CompleteBatchRepresentation(entryStart, entries, commitEntry);
            } else {
                current = ChunkedBatchRepresentation.createChunkRepresentation(startEntry, entries, endEntry);
            }
        } else {
            throw new IllegalStateException("Was expecting transaction or chunk start but got: " + entry);
        }
        channel.getCurrentLogPosition(lastGoodPositionMarker);
        return true;
    }

    @Override
    public void close() throws IOException {
        logEntryCursor.close();
    }

    /**
     * @return last known good position, which is a {@link LogPosition} after a {@link LogEntryCommit}.
     */
    @Override
    public LogPosition position() {
        return lastGoodPositionMarker.newPosition();
    }
}
