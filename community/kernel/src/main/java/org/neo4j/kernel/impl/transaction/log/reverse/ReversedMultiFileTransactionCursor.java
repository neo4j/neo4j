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
package org.neo4j.kernel.impl.transaction.log.reverse;

import java.io.IOException;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;

/**
 * Similar to {@link PhysicalTransactionCursor} and actually uses it internally. This main difference is that transactions
 * are returned in reverse order, starting from the end and back towards (and including) a specified {@link LogPosition}.
 *
 * Since the transaction log format lacks data which would allow for a memory efficient reverse reading implementation,
 * this implementation tries to minimize peak memory consumption by efficiently reading a single log version at a time
 * in reverse order before moving over to the previous version. Peak memory consumption compared to normal
 * {@link PhysicalTransactionCursor} should be negligible due to the offset mapping that {@link ReversedSingleFileTransactionCursor}
 * does.
 *
 * @see ReversedSingleFileTransactionCursor
 */
public class ReversedMultiFileTransactionCursor implements TransactionCursor {
    private final TransactionCursors transactionCursors;
    private TransactionCursor currentLogTransactionCursor;

    /**
     * Utility method for creating a {@link ReversedMultiFileTransactionCursor} with a {@link LogFile} as the source of
     * {@link TransactionCursor} for each log version.
     *
     * @param logFile accessor of log files.
     * @param backToPosition {@link LogPosition} to read backwards to.
     * @param logEntryReader {@link LogEntryReader} to use.
     * @param failOnCorruptedLogFiles fail reading from log files as soon as first error is encountered
     * @param monitor reverse transaction cursor monitor
     * @param presketch enables pre-sketching of next transaction file.
     * @return a {@link TransactionCursor} which returns transactions from the end of the log stream and backwards to
     * and including transaction starting at {@link LogPosition}.
     */
    public static TransactionCursor fromLogFile(
            LogFile logFile,
            LogPosition backToPosition,
            LogEntryReader logEntryReader,
            boolean failOnCorruptedLogFiles,
            ReversedTransactionCursorMonitor monitor,
            boolean presketch) {
        if (presketch) {
            return new ReversedMultiFileTransactionCursor(new PrefetchedTransactionCursors(
                    logFile, backToPosition, logEntryReader, failOnCorruptedLogFiles, monitor));
        } else {
            return new ReversedMultiFileTransactionCursor(new DefaultTransactionCursors(
                    logFile, backToPosition, logEntryReader, failOnCorruptedLogFiles, monitor));
        }
    }

    public ReversedMultiFileTransactionCursor(TransactionCursors transactionCursors) {
        this.transactionCursors = transactionCursors;
    }

    @Override
    public CommittedTransactionRepresentation get() {
        return currentLogTransactionCursor.get();
    }

    @Override
    public boolean next() throws IOException {
        while (currentLogTransactionCursor == null || !currentLogTransactionCursor.next()) {
            var cursor = transactionCursors.next();
            if (cursor.isEmpty()) {
                return false;
            }
            closeCurrent();
            currentLogTransactionCursor = cursor.get();
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        closeCurrent();
        transactionCursors.close();
    }

    private void closeCurrent() throws IOException {
        if (currentLogTransactionCursor != null) {
            currentLogTransactionCursor.close();
            currentLogTransactionCursor = null;
        }
    }

    @Override
    public LogPosition position() {
        return currentLogTransactionCursor.position();
    }
}
