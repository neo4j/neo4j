/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.xa.LogDeserializer;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandReaderFactory;
import org.neo4j.kernel.impl.nioneo.xa.XaCommandWriterFactory;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.Consumer;
import org.neo4j.kernel.impl.util.Cursor;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * During log rotation, any unfinished transactions in the current log need to be copied over to the
 * new log. Correctly performing that copy is the responsibility of this class.
 */
class PartialTransactionCopier
{
    private final ByteBuffer sharedBuffer;
    private final LogEntryWriterv1 logEntryWriter;
    private final XaCommandReaderFactory commandReaderFactory;
    private final XaCommandWriterFactory commandWriterFactory;
    private final StringLogger log;
    private final LogExtractor.LogPositionCache positionCache;
    private final LogExtractor.LogLoader logLoader;
    private final ArrayMap<Integer,LogEntry.Start> xidIdentMap;
    private final EntireTransactionCopyingConsumer copyEntireTransactions;

    PartialTransactionCopier( ByteBuffer sharedBuffer, XaCommandReaderFactory commandReaderFactory,
                              XaCommandWriterFactory commandWriterFactory, StringLogger log,
                              LogExtractor.LogPositionCache positionCache, LogExtractor.LogLoader logLoader,
                              LogEntryWriterv1 logEntryWriter, ArrayMap<Integer, LogEntry.Start> xidIdentMap )
    {
        this.sharedBuffer = sharedBuffer;
        this.logEntryWriter = logEntryWriter;
        this.commandReaderFactory = commandReaderFactory;
        this.commandWriterFactory = commandWriterFactory;
        this.log = log;
        this.positionCache = positionCache;
        this.logLoader = logLoader;
        this.xidIdentMap = xidIdentMap;
        this.copyEntireTransactions = new EntireTransactionCopyingConsumer();
    }

    public void copy( StoreChannel sourceLog, LogBuffer targetLog, long targetLogVersion ) throws IOException
    {
        LogDeserializer deserializer = new LogDeserializer( sharedBuffer, commandReaderFactory );
        copyEntireTransactions.init( targetLog, targetLogVersion );
        Cursor<LogEntry, IOException> cursor = deserializer.cursor( sourceLog );
        while( cursor.next( copyEntireTransactions ) ); // let exceptions propagate, the channel is closed outside
    }

    private class EntireTransactionCopyingConsumer implements Consumer<LogEntry, IOException>
    {
        private final Map<Integer,LogEntry.Start> startEntriesEncountered = new HashMap<>();
        private final CopyUntilCommitConsumer copyUntilCommitEntry = new CopyUntilCommitConsumer();
        private boolean foundFirstActiveTx;

        private LogBuffer targetLog;
        private long targetLogVersion;

        public void init( LogBuffer targetLog, long targetLogVersion )
        {
            this.targetLog = targetLog;
            this.targetLogVersion = targetLogVersion;
            foundFirstActiveTx = false;
            startEntriesEncountered.clear();
        }

        @Override
        public boolean accept( LogEntry logEntry ) throws IOException
        {
            Integer identifier = logEntry.getIdentifier();
            boolean isActive = xidIdentMap.get( identifier ) != null;
            if ( !foundFirstActiveTx && isActive )
            {
                foundFirstActiveTx = true;
            }

            if ( foundFirstActiveTx )
            {
                if ( logEntry instanceof LogEntry.Start )
                {
                    LogEntry.Start startEntry = (LogEntry.Start) logEntry;
                    startEntriesEncountered.put( identifier, startEntry );
                    startEntry.setStartPosition( targetLog.getFileChannelPosition() );
                    // If the transaction is active then update it with the new one
                    if ( isActive ) xidIdentMap.put( identifier, startEntry );
                }
                else if ( logEntry instanceof LogEntry.Commit )
                {
                    LogEntry.Commit commitEntry = (LogEntry.Commit) logEntry;
                    LogEntry.Start startEntry = startEntriesEncountered.get( identifier );
                    if ( startEntry == null )
                    {
                        // Fetch from log extractor instead (all entries except commit (and done) records, which will be copied from the source).
                        startEntry = extractTransactionFromEarlierInLog( commitEntry.getTxId() );
                        startEntriesEncountered.put( identifier, startEntry );
                    }
                    else
                    {
                        LogExtractor.TxPosition oldPos = positionCache.getStartPosition( commitEntry.getTxId() );
                        LogExtractor.TxPosition newPos = positionCache.cacheStartPosition( commitEntry.getTxId(), startEntry, targetLogVersion );
                        log.logMessage( "Updated tx " + ((LogEntry.Commit) logEntry ).getTxId() +
                                " from " + oldPos + " to " + newPos );
                    }
                }
                if ( startEntriesEncountered.containsKey( identifier ) )
                {
                    logEntryWriter.writeLogEntry( logEntry, targetLog );
                }
            }
            return true;
        }

        private LogEntry.Start extractTransactionFromEarlierInLog( long txId ) throws IOException
        {
            LogExtractor extractor = new LogExtractor( positionCache, logLoader, commandReaderFactory,
                    commandWriterFactory, logEntryWriter, txId, txId );

            try ( Cursor<LogEntry, IOException> cursor = extractor.cursor( new InMemoryLogBuffer() ) )
            {
                while ( cursor.next( copyUntilCommitEntry ) );
            }

            return extractor.getLastStartEntry();
        }

        private class CopyUntilCommitConsumer implements Consumer<LogEntry, IOException>
        {
            @Override
            public boolean accept( LogEntry entry ) throws IOException
            {
                if ( entry instanceof LogEntry.Commit )
                {
                    return false;
                }
                logEntryWriter.writeLogEntry( entry, targetLog );
                return true;
            }
        }
    }
}
