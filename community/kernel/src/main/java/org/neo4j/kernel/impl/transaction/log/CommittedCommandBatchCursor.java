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
import org.neo4j.kernel.impl.transaction.CommittedChunkRepresentation;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.RollbackChunkRepresentation;
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
    private final boolean light;
    private final LogEntryCursor logEntryCursor;
    private final LogPositionMarker lastGoodPositionMarker = new LogPositionMarker();

    private CommittedCommandBatch current;

    public CommittedCommandBatchCursor(ReadableLogPositionAwareChannel channel, LogEntryReader entryReader)
            throws IOException {
        this(channel, entryReader, false);
    }

    /**
     * @param channel       channel to read
     * @param entryReader   entry reader
     * @param light         if true, actual commands will not be included into the batches
     * @throws IOException  if something goes wrong
     */
    public CommittedCommandBatchCursor(
            ReadableLogPositionAwareChannel channel, LogEntryReader entryReader, boolean light) throws IOException {
        this.channel = channel;
        this.light = light;
        channel.getCurrentLogPosition(lastGoodPositionMarker);
        this.logEntryCursor = new LogEntryCursor(entryReader, channel);
    }

    @Override
    public CommittedCommandBatch get() {
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
            current = new RollbackChunkRepresentation(
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
                if (!light) {
                    entries.add(command.getCommand());
                }
            }
            if (startEntry instanceof LogEntryStart entryStart && endEntry instanceof LogEntryCommit commitEntry) {
                current = new CommittedTransactionRepresentation(entryStart, entries, commitEntry);
            } else {
                current = CommittedChunkRepresentation.createChunkRepresentation(startEntry, entries, endEntry);
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
