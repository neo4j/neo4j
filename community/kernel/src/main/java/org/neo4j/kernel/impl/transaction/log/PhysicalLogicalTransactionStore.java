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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogHeaderVisitor;
import org.neo4j.kernel.impl.transaction.log.reverse.ReversedMultiFileTransactionCursor;
import org.neo4j.kernel.impl.transaction.log.reverse.ReversedTransactionCursorMonitor;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.CommandReaderFactory;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.pre_sketch_transaction_logs;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_COMMIT;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;

public class PhysicalLogicalTransactionStore implements LogicalTransactionStore
{
    private final LogFile logFile;
    private final TransactionMetadataCache transactionMetadataCache;
    private final CommandReaderFactory commandReaderFactory;
    private final Monitors monitors;
    private final boolean failOnCorruptedLogFiles;
    private final boolean presketchLogFiles;

    public PhysicalLogicalTransactionStore( LogFiles logFiles,
                                            TransactionMetadataCache transactionMetadataCache,
                                            CommandReaderFactory commandReaderFactory, Monitors monitors,
                                            boolean failOnCorruptedLogFiles, Config config )
    {
        this.logFile = logFiles.getLogFile();
        this.transactionMetadataCache = transactionMetadataCache;
        this.commandReaderFactory = commandReaderFactory;
        this.monitors = monitors;
        this.failOnCorruptedLogFiles = failOnCorruptedLogFiles;
        this.presketchLogFiles = config.get( pre_sketch_transaction_logs );
    }

    @Override
    public TransactionCursor getTransactions( LogPosition position ) throws IOException
    {
        return new PhysicalTransactionCursor( logFile.getReader( position ), new VersionAwareLogEntryReader( commandReaderFactory ) );
    }

    @Override
    public TransactionCursor getTransactionsInReverseOrder( LogPosition backToPosition )
    {
        return ReversedMultiFileTransactionCursor
                .fromLogFile( logFile, backToPosition, new VersionAwareLogEntryReader( commandReaderFactory ), failOnCorruptedLogFiles,
                        monitors.newMonitor( ReversedTransactionCursorMonitor.class ), presketchLogFiles );
    }

    @Override
    public TransactionCursor getTransactions( final long transactionIdToStartFrom ) throws IOException
    {
        // look up in position cache
        try
        {
            var logEntryReader = new VersionAwareLogEntryReader( commandReaderFactory );
            TransactionMetadataCache.TransactionMetadata transactionMetadata = transactionMetadataCache.getTransactionMetadata( transactionIdToStartFrom );
            if ( transactionMetadata != null )
            {
                // we're good
                var channel = logFile.getReader( transactionMetadata.getStartPosition() );
                return new PhysicalTransactionCursor( channel, logEntryReader );
            }

            // ask logFiles about the version it may be in
            var headerVisitor = new LogVersionLocator( transactionIdToStartFrom );
            logFile.accept( headerVisitor );

            // ask LogFile
            var transactionPositionLocator = new TransactionPositionLocator( transactionIdToStartFrom, logEntryReader );
            logFile.accept( transactionPositionLocator, headerVisitor.getLogPosition() );
            var position = transactionPositionLocator.getLogPosition();
            transactionMetadataCache.cacheTransactionMetadata( transactionIdToStartFrom, position );
            return new PhysicalTransactionCursor( logFile.getReader( position ), logEntryReader );
        }
        catch ( NoSuchFileException e )
        {
            throw new NoSuchTransactionException(
                    transactionIdToStartFrom,
                    "Log position acquired, but couldn't find the log file itself. Perhaps it just recently was " +
                    "deleted? [" + e.getMessage() + "]",
                    e );
        }
    }

    public static class TransactionPositionLocator implements LogFile.LogFileVisitor
    {
        private final long startTransactionId;
        private final LogEntryReader logEntryReader;
        private LogEntryStart transactionStartEntry;

        TransactionPositionLocator( long startTransactionId, LogEntryReader logEntryReader )
        {
            this.startTransactionId = startTransactionId;
            this.logEntryReader = logEntryReader;
        }

        @Override
        public boolean visit( ReadableClosablePositionAwareChecksumChannel channel ) throws IOException
        {
            LogEntry logEntry;
            LogEntryStart startEntry = null;
            while ( (logEntry = logEntryReader.readLogEntry( channel )) != null )
            {
                switch ( logEntry.getType() )
                {
                case TX_START:
                    startEntry = (LogEntryStart) logEntry;
                    break;
                case TX_COMMIT:
                    LogEntryCommit commit = (LogEntryCommit) logEntry;
                    if ( commit.getTxId() == startTransactionId )
                    {
                        transactionStartEntry = startEntry;
                        return false;
                    }
                    break;
                default: // just skip commands
                    break;
                }
            }
            return true;
        }

        LogPosition getLogPosition() throws NoSuchTransactionException
        {
            if ( transactionStartEntry == null )
            {
                throw new NoSuchTransactionException( startTransactionId );
            }
            return transactionStartEntry.getStartPosition();
        }
    }

    public static final class LogVersionLocator implements LogHeaderVisitor
    {
        private final long transactionId;
        private LogPosition foundPosition;

        public LogVersionLocator( long transactionId )
        {
            this.transactionId = transactionId;
        }

        @Override
        public boolean visit( LogHeader logHeader, LogPosition position, long firstTransactionIdInLog, long lastTransactionIdInLog )
        {
            boolean foundIt = transactionId >= firstTransactionIdInLog &&
                              transactionId <= lastTransactionIdInLog;
            if ( foundIt )
            {
                foundPosition = position;
            }
            return !foundIt; // continue as long we don't find it
        }

        public LogPosition getLogPosition() throws NoSuchTransactionException
        {
            if ( foundPosition == null )
            {
                throw new NoSuchTransactionException( transactionId, "Couldn't find any log containing " + transactionId );
            }
            return foundPosition;
        }
    }
}
