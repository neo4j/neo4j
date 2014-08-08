/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.xaframework.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.nioneo.store.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.kernel.impl.transaction.xaframework.log.entry.VersionAwareLogEntryReader.LOG_HEADER_SIZE;

public class PhysicalLogicalTransactionStore extends LifecycleAdapter implements LogicalTransactionStore
{
    private final LogFile logFile;
    private final TransactionMetadataCache transactionMetadataCache;
    private final TxIdGenerator txIdGenerator;
    private TransactionAppender appender;
    private final TransactionIdStore transactionIdStore;

    public PhysicalLogicalTransactionStore( LogFile logFile, TxIdGenerator txIdGenerator,
            TransactionMetadataCache transactionMetadataCache,
            TransactionIdStore transactionIdStore )
    {
        this.logFile = logFile;
        this.txIdGenerator = txIdGenerator;
        this.transactionMetadataCache = transactionMetadataCache;
        this.transactionIdStore = transactionIdStore;
    }

    @Override
    public void init() throws Throwable
    {
        this.appender = new PhysicalTransactionAppender( logFile, txIdGenerator, transactionMetadataCache,
                transactionIdStore );
    }

    @Override
    public TransactionAppender getAppender()
    {
        return appender;
    }

    @Override
    public IOCursor<CommittedTransactionRepresentation> getTransactions( final long transactionIdToStartFrom )
            throws IOException
    {
        // look up in position cache
        try
        {
            TransactionMetadataCache.TransactionMetadata transactionMetadata =
                    transactionMetadataCache.getTransactionMetadata( transactionIdToStartFrom );
            LogEntryReader<ReadableLogChannel> logEntryReader = new VersionAwareLogEntryReader();
            if ( transactionMetadata != null )
            {
                // we're good
                return new PhysicalTransactionCursor( logFile.getReader( transactionMetadata.getStartPosition() ),
                        logEntryReader );
            }

            // ask LogFile about the version it may be in
            LogVersionLocator headerVisitor = new LogVersionLocator( transactionIdToStartFrom );
            logFile.accept( headerVisitor );

            // ask LogFile
            TransactionPositionLocator transactionPositionLocator =
                    new TransactionPositionLocator( transactionIdToStartFrom, logEntryReader );
            logFile.accept( transactionPositionLocator, headerVisitor.getLogPosition() );
            LogPosition position = transactionPositionLocator.getAndCacheFoundLogPosition( transactionMetadataCache );
            return new PhysicalTransactionCursor( logFile.getReader( position ), logEntryReader );
        }
        catch ( FileNotFoundException e )
        {
            throw new NoSuchTransactionException( transactionIdToStartFrom,
                    "Log position acquired, but couldn't find the log file itself. Perhaps it just recently was deleted?" );
        }
    }

    private static final TransactionMetadataCache.TransactionMetadata METADATA_FOR_EMPTY_STORE =
            new TransactionMetadataCache.TransactionMetadata( -1, -1, new LogPosition( 0, LOG_HEADER_SIZE ), 0 );

    @Override
    public TransactionMetadataCache.TransactionMetadata getMetadataFor( long transactionId ) throws IOException
    {
        if ( transactionId <= BASE_TX_ID )
        {
            return METADATA_FOR_EMPTY_STORE;
        }

        TransactionMetadataCache.TransactionMetadata transactionMetadata =
                transactionMetadataCache.getTransactionMetadata( transactionId );
        if ( transactionMetadata == null )
        {
            try (IOCursor<CommittedTransactionRepresentation> cursor = getTransactions( transactionId ))
            {
                while (cursor.next())
                {
                    CommittedTransactionRepresentation tx = cursor.get();
                    transactionMetadataCache.cacheTransactionMetadata( tx.getCommitEntry().getTxId(),
                            tx.getStartEntry().getStartPosition(), tx.getStartEntry().getMasterId(),
                            tx.getStartEntry().getLocalId(), LogEntryStart.checksum( tx.getStartEntry() ) );

                };
            }
        }
        transactionMetadata = transactionMetadataCache.getTransactionMetadata( transactionId );
        return transactionMetadata;
    }

    public static class TransactionPositionLocator implements LogFile.LogFileVisitor
    {
        private final long startTransactionId;
        private final LogEntryReader<ReadableLogChannel> logEntryReader;
        private LogEntryStart startEntryForFoundTransaction;

        public TransactionPositionLocator( long startTransactionId, LogEntryReader<ReadableLogChannel> logEntryReader )
        {
            this.startTransactionId = startTransactionId;
            this.logEntryReader = logEntryReader;
        }

        @Override
        public boolean visit( LogPosition position, ReadableLogChannel channel ) throws IOException
        {
            LogEntry logEntry;
            LogEntryStart startEntry = null;
            while ( (logEntry = logEntryReader.readLogEntry( channel ) ) != null )
            {
                switch ( logEntry.getType() )
                {
                    case LogEntry.TX_START:
                        startEntry = (LogEntryStart) logEntry;
                        break;
                    case LogEntry.TX_1P_COMMIT:
                        LogEntryCommit commit = (LogEntryCommit) logEntry;
                        if ( commit.getTxId() == startTransactionId )
                        {
                            startEntryForFoundTransaction = startEntry;
                            return false;
                        }
                    default: // just skip commands
                        break;
                }
            }
            return true;
        }

        public LogPosition getAndCacheFoundLogPosition( TransactionMetadataCache transactionMetadataCache )
                throws NoSuchTransactionException
        {
            if ( startEntryForFoundTransaction == null )
            {
                throw new NoSuchTransactionException( startTransactionId );
            }
            transactionMetadataCache.cacheTransactionMetadata(
                    startTransactionId,
                    startEntryForFoundTransaction.getStartPosition(),
                    startEntryForFoundTransaction.getMasterId(),
                    startEntryForFoundTransaction.getLocalId(),
                    LogEntryStart.checksum( startEntryForFoundTransaction )
            );
            return startEntryForFoundTransaction.getStartPosition();
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
        public boolean visit( LogPosition position, long firstTransactionIdInLog, long lastTransactionIdInLog )
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
                throw new NoSuchTransactionException( transactionId, "Couldn't find any log containing " +
                        transactionId );
            }
            return foundPosition;
        }
    }
}
