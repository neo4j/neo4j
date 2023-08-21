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

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;

import java.io.IOException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;

public class TransactionOrEndPositionLocator implements LogFile.LogFileVisitor {
    private final long startTransactionId;
    private final LogEntryReader logEntryReader;
    private LogPosition position;

    public TransactionOrEndPositionLocator(long startTransactionId, LogEntryReader logEntryReader) {
        this.startTransactionId = startTransactionId;
        this.logEntryReader = logEntryReader;
    }

    @Override
    public boolean visit(ReadableLogPositionAwareChannel channel) throws IOException {
        LogEntry logEntry;
        LogEntryStart startEntry = null;
        while ((logEntry = logEntryReader.readLogEntry(channel)) != null) {
            switch (logEntry.getType()) {
                case TX_START -> startEntry = (LogEntryStart) logEntry;
                case TX_COMMIT -> {
                    LogEntryCommit commit = (LogEntryCommit) logEntry;
                    if (commit.getTxId() == startTransactionId) {
                        if (startEntry == null) {
                            throw new IllegalStateException("Commit log entry wasn't proceeded by a start log entry.");
                        }
                        position = startEntry.getStartPosition();
                        return false;
                    }
                }
                default -> {} // just skip commands
            }
        }

        position = channel.getCurrentLogPosition();
        return true;
    }

    public LogPosition getLogPosition() throws NoSuchTransactionException {
        if (position == null) {
            throw new NoSuchTransactionException(startTransactionId);
        }
        return position;
    }
}
