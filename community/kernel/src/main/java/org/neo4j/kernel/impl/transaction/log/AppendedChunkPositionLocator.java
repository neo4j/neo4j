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

import static org.neo4j.kernel.KernelVersion.VERSION_APPEND_INDEX_INTRODUCED;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.CHUNK_START;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_ROLLBACK;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;

import java.io.IOException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryChunkStart;
import org.neo4j.kernel.impl.transaction.log.entry.v57.LogEntryRollback;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;

public class AppendedChunkPositionLocator implements LogFile.LogFileVisitor {
    private final long appendIndex;
    private final LogEntryReader logEntryReader;
    private LogPosition position;

    public AppendedChunkPositionLocator(long appendIndex, LogEntryReader logEntryReader) {
        this.appendIndex = appendIndex;
        this.logEntryReader = logEntryReader;
    }

    @Override
    public boolean visit(ReadableLogPositionAwareChannel channel) throws IOException {
        LogPosition lastStartPosition = null;
        LogEntry logEntry;
        do {
            var logPosition = channel.getCurrentLogPosition();
            logEntry = logEntryReader.readLogEntry(channel);
            if (logEntry != null) {
                switch (logEntry.getType()) {
                    case TX_START -> {
                        if (logEntry instanceof LogEntryStart entryStart) {
                            if (entryStart.kernelVersion().isAtLeast(VERSION_APPEND_INDEX_INTRODUCED)
                                    && (entryStart.getAppendIndex() == appendIndex)) {
                                position = logPosition;
                                return false;
                            }
                            lastStartPosition = logPosition;
                        }
                    }
                    case CHUNK_START -> {
                        if (logEntry instanceof LogEntryChunkStart chunkStart) {
                            if (chunkStart.getAppendIndex() == appendIndex) {
                                position = logPosition;
                                return false;
                            }
                        }
                    }
                    case TX_ROLLBACK -> {
                        if (logEntry instanceof LogEntryRollback rollback) {
                            if (rollback.getAppendIndex() == appendIndex) {
                                position = logPosition;
                                return false;
                            }
                        }
                    }
                    case TX_COMMIT -> {
                        if (logEntry instanceof LogEntryCommit commit) {
                            if (commit.kernelVersion().isLessThan(VERSION_APPEND_INDEX_INTRODUCED)
                                    && (commit.getTxId() == appendIndex)) {
                                position = lastStartPosition;
                                return false;
                            }
                        }
                    }
                    default -> {} // just skip commands
                }
            }
        } while (logEntry != null);

        position = channel.getCurrentLogPosition();
        return true;
    }

    public LogPosition getLogPositionOrThrow() throws NoSuchLogEntryException {
        if (position == null) {
            throw new NoSuchLogEntryException(appendIndex);
        }
        return position;
    }
}
