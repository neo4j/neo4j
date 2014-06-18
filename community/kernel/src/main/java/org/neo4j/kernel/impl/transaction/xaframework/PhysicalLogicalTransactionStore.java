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

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class PhysicalLogicalTransactionStore extends LifecycleAdapter implements LogicalTransactionStore
{
    private final LogFile logFile;
    private final TransactionMetadataCache transactionMetadataCache;
    private final TxIdGenerator txIdGenerator;
    private TransactionAppender appender;
    private final LogEntryReader<ReadableLogChannel> logEntryReader;

    public PhysicalLogicalTransactionStore( LogFile logFile, TxIdGenerator txIdGenerator, TransactionMetadataCache
            transactionMetadataCache,
                                            LogEntryReader<ReadableLogChannel> logEntryReader )
    {
        this.logFile = logFile;
        this.txIdGenerator = txIdGenerator;
        this.transactionMetadataCache = transactionMetadataCache;
        this.logEntryReader = logEntryReader;
    }

    @Override
    public void init() throws Throwable
    {
        this.appender = new PhysicalTransactionAppender( logFile, txIdGenerator, transactionMetadataCache );
    }

    @Override
    public void shutdown() throws Throwable
    {
        if ( appender != null )
        {
            appender.close();
            appender = null;
        }
    }

    @Override
    public TransactionAppender getAppender()
    {
        return appender;
    }

    @Override
    public IOCursor getCursor( long transactionIdToStartFrom,
            Visitor<CommittedTransactionRepresentation, IOException> visitor )
            throws NoSuchTransactionException, IOException
    {
        // look up in position cache
        LogPosition position = transactionMetadataCache.getTransactionMetadata( transactionIdToStartFrom ).getStartPosition();
        if ( position != null )
        {
            // we're good
            return new PhysicalTransactionCursor( logFile.getReader( position ), logEntryReader, visitor );
        }

        // ask LogFile
        TransactionPositionLocator transactionPositionLocator = new TransactionPositionLocator( transactionIdToStartFrom );
        logFile.accept( transactionPositionLocator);
        position = transactionPositionLocator.getPosition();
        // TODO 2.2-future play forward and cache that position
        IOCursor cursor = new PhysicalTransactionCursor( logFile.getReader( position ), logEntryReader, visitor );
        return cursor;
    }

    @Override
    public TransactionMetadataCache.TransactionMetadata getMetadataFor( long transactionId )
    {
        TransactionMetadataCache.TransactionMetadata transactionMetadata = transactionMetadataCache
                .getTransactionMetadata( transactionId );
        if ( transactionMetadata == null )
        {
            logFile.accept( new TransactionMetadataFiller( transactionId ) );
        }
        transactionMetadata = transactionMetadataCache
                .getTransactionMetadata( transactionId );
        return transactionMetadata;
    }

    @Override
    public void close() throws IOException
    {
    }

    private static class TransactionPositionLocator implements LogFile.LogFileVisitor
    {
        private final long startTransactionId;
        private LogPosition position;

        TransactionPositionLocator( long startTransactionId )
        {
            this.startTransactionId = startTransactionId;
        }

        @Override
        public boolean visit( LogPosition position, ReadableLogChannel channel )
        {
            // TODO Auto-generated method stub
            return false;
        }

        public LogPosition getPosition()
        {
            return position;
        }
    }

    private class TransactionMetadataFiller implements LogFile.LogFileVisitor
    {
        // TODO 2.2-future this is supposed to fill the metadata cache with information about the passed tx
        public TransactionMetadataFiller( long transactionId )
        {
        }

        @Override
        public boolean visit( LogPosition position, ReadableLogChannel channel )
        {
            // TODO Auto-generated method stub
            return false;
        }
    }
}
