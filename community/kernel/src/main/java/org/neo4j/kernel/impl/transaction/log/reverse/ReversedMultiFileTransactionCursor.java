/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.function.ThrowingFunction;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;

import static org.neo4j.kernel.impl.transaction.log.LogPosition.start;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.reverse.EagerlyReversedTransactionCursor.eagerlyReverse;

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
public class ReversedMultiFileTransactionCursor implements TransactionCursor
{
    private final LogPosition backToPosition;
    private final ThrowingFunction<LogPosition,TransactionCursor,IOException> cursorFactory;

    private long currentVersion;
    private TransactionCursor currentLogTransactionCursor;

    /**
     * Utility method for creating a {@link ReversedMultiFileTransactionCursor} with a {@link LogFile} as the source of
     * {@link TransactionCursor} for each log version.
     *
     * @param logFile {@link LogFile} to supply log entries forming transactions.
     * @param backToPosition {@link LogPosition} to read backwards to.
     * @param failOnCorruptedLogFiles fail reading from log files as soon as first error is encountered
     * @param monitor reverse transaction cursor monitor
     * @return a {@link TransactionCursor} which returns transactions from the end of the log stream and backwards to
     * and including transaction starting at {@link LogPosition}.
     * @throws IOException on I/O error.
     */
    public static TransactionCursor fromLogFile( LogFiles logFiles, LogFile logFile, LogPosition backToPosition,
            boolean failOnCorruptedLogFiles, ReversedTransactionCursorMonitor monitor )
    {
        long highestVersion = logFiles.getHighestLogVersion();
        LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        ThrowingFunction<LogPosition,TransactionCursor,IOException> factory = position ->
        {
            ReadableLogChannel channel = logFile.getReader( position, NO_MORE_CHANNELS );
            if ( channel instanceof ReadAheadLogChannel )
            {
                // This is a channel which can be positioned explicitly and is the typical case for such channels
                // Let's take advantage of this fact and use a bit smarter reverse implementation
                return new ReversedSingleFileTransactionCursor( (ReadAheadLogChannel) channel, logEntryReader,
                        failOnCorruptedLogFiles, monitor );
            }

            // Fall back to simply eagerly reading each single log file and reversing in memory
            return eagerlyReverse( new PhysicalTransactionCursor<>( channel, logEntryReader ) );
        };
        return new ReversedMultiFileTransactionCursor( factory, highestVersion, backToPosition );
    }

    /**
     * @param cursorFactory creates {@link TransactionCursor} from a given {@link LogPosition}. The returned cursor must
     * return transactions from the end of that {@link LogPosition#getLogVersion() log version} and backwards in reverse order
     * to, and including, the transaction at the {@link LogPosition} given to it.
     * @param highestVersion highest log version right now.
     * @param backToPosition the start position of the last transaction to return from this cursor.
     */
    ReversedMultiFileTransactionCursor( ThrowingFunction<LogPosition,TransactionCursor,IOException> cursorFactory, long highestVersion,
            LogPosition backToPosition )
    {
        this.cursorFactory = cursorFactory;
        this.backToPosition = backToPosition;
        this.currentVersion = highestVersion + 1;
    }

    @Override
    public CommittedTransactionRepresentation get()
    {
        return currentLogTransactionCursor.get();
    }

    @Override
    public boolean next() throws IOException
    {
        while ( currentLogTransactionCursor == null || !currentLogTransactionCursor.next() )
        {
            // We've run out of transactions in this log version, back up to a previous one
            currentVersion--;
            if ( currentVersion < backToPosition.getLogVersion() )
            {
                return false;
            }

            closeCurrent();
            LogPosition position = currentVersion > backToPosition.getLogVersion() ? start( currentVersion ) : backToPosition;
            currentLogTransactionCursor = cursorFactory.apply( position );
        }
        return true;
    }

    @Override
    public void close() throws IOException
    {
        closeCurrent();
    }

    private void closeCurrent() throws IOException
    {
        if ( currentLogTransactionCursor != null )
        {
            currentLogTransactionCursor.close();
            currentLogTransactionCursor = null;
        }
    }

    @Override
    public LogPosition position()
    {
        return currentLogTransactionCursor.position();
    }
}
