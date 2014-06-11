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

public class PhysicalTransactionStore extends LifecycleAdapter implements TransactionStore
{
    private final LogFile logFile;
    private final LogPositionCache positionCache;
    private final TxIdGenerator txIdGenerator;
    private TransactionAppender appender;
    private final LogEntryReader<ReadableLogChannel> logEntryReader;

    public PhysicalTransactionStore( LogFile logFile, TxIdGenerator txIdGenerator, LogPositionCache positionCache,
            LogEntryReader<ReadableLogChannel> logEntryReader)
    {
        this.logFile = logFile;
        this.txIdGenerator = txIdGenerator;
        this.positionCache = positionCache;
        this.logEntryReader = logEntryReader;
    }

    @Override
    public void init() throws Throwable
    {
        this.appender = new PhysicalTransactionAppender( logFile.getWriter(), txIdGenerator, positionCache );
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
    public TransactionCursor getCursor( long transactionIdToStartFrom, Visitor<TransactionRepresentation, IOException> visitor ) throws NoSuchTransactionException, IOException
    {
        // look up in position cache
        LogPosition position = positionCache.getStartPosition( transactionIdToStartFrom );
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
        TransactionCursor cursor = new PhysicalTransactionCursor( logFile.getReader( position ), logEntryReader, visitor );
        return cursor;
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
}
