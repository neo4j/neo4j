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
import org.neo4j.kernel.api.exceptions.TransactionFailureException;

public class PhysicalTransactionStore implements TransactionStore
{
    private final LogFile logFile;
    private final LogPositionCache positionCache;
    private final TxIdGenerator txIdGenerator;
    private PhysicalTransactionAppender appender;
    private final LogEntryReader<ReadableLogChannel> logEntryReader;

    public PhysicalTransactionStore( LogFile logFile, TxIdGenerator txIdGenerator, LogPositionCache positionCache,
            LogEntryReader<ReadableLogChannel> logEntryReader )
    {
        this.logFile = logFile;
        this.txIdGenerator = txIdGenerator;
        this.positionCache = positionCache;
        this.logEntryReader = logEntryReader;
    }

    @Override
    public void open( Visitor<TransactionRepresentation, TransactionFailureException> recoveredTransactionVisitor )
            throws IOException, TransactionFailureException
    {
        logFile.open( new Visitor<ReadableLogChannel, IOException>()
        {
            @Override
            public boolean visit( ReadableLogChannel element ) throws IOException
            {
                // while more data
                //  read next transaction
                //  hand it over to recoveredTransactionVisitor
                throw new UnsupportedOperationException( "Please implement" );
            }
        } );

        this.appender = new PhysicalTransactionAppender( logFile.getWriter(), txIdGenerator, positionCache );
    }

    @Override
    public TransactionAppender getAppender()
    {
        return appender;
    }

    @Override
    public TransactionCursor getCursor( long transactionIdToStartFrom ) throws NoSuchTransactionException, IOException
    {
        // look up in position cache
        LogPosition position = positionCache.getStartPosition( transactionIdToStartFrom );
        if ( position != null )
        {
            // we're good
            return new PhysicalTransactionCursor( logFile.getReader( position ), logEntryReader );
        }

        // ask LogFile
        position = logFile.findRoughPositionOf( transactionIdToStartFrom );
        // TODO 2.2-future play forward and cache that position
        TransactionCursor cursor = new PhysicalTransactionCursor( logFile.getReader( position ), logEntryReader );
        return cursor;
    }
}
