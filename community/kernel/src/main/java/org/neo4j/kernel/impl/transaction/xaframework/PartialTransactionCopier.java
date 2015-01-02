/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

import static org.neo4j.kernel.impl.transaction.xaframework.LogExtractor.newLogReaderBuffer;

/**
 * During log rotation, any unfinished transactions in the current log need to be copied over to the
 * new log. Correctly performing that copy is the responsibility of this class.
 */
class PartialTransactionCopier
{
    private final ByteBuffer sharedBuffer;
    private final XaCommandFactory commandFactory;
    private final StringLogger log;
    private final LogExtractor.LogPositionCache positionCache;
    private final LogExtractor.LogLoader logLoader;
    private final ArrayMap<Integer,LogEntry.Start> xidIdentMap;
    private final ByteCounterMonitor monitor;

    PartialTransactionCopier( ByteBuffer sharedBuffer, XaCommandFactory commandFactory, StringLogger log,
                              LogExtractor.LogPositionCache positionCache, LogExtractor.LogLoader logLoader,
                              ArrayMap<Integer, LogEntry.Start> xidIdentMap, ByteCounterMonitor monitor )
    {
        this.sharedBuffer = sharedBuffer;
        this.commandFactory = commandFactory;
        this.log = log;
        this.positionCache = positionCache;
        this.logLoader = logLoader;
        this.xidIdentMap = xidIdentMap;
        this.monitor = monitor;
    }

    public void copy( StoreChannel sourceLog, LogBuffer targetLog, long targetLogVersion ) throws IOException
    {
        boolean foundFirstActiveTx = false;
        Map<Integer,LogEntry.Start> startEntriesEncountered = new HashMap<Integer,LogEntry.Start>();
        for ( LogEntry entry = null; (entry = LogIoUtils.readEntry( sharedBuffer, sourceLog, commandFactory )) != null; )
        {
            Integer identifier = entry.getIdentifier();
            boolean isActive = xidIdentMap.get( identifier ) != null;
            if ( !foundFirstActiveTx && isActive )
            {
                foundFirstActiveTx = true;
            }

            if ( foundFirstActiveTx )
            {
                if ( entry instanceof LogEntry.Start )
                {
                    LogEntry.Start startEntry = (LogEntry.Start) entry;
                    startEntriesEncountered.put( identifier, startEntry );
                    startEntry.setStartPosition( targetLog.getFileChannelPosition() );
                    // If the transaction is active then update it with the new one
                    if ( isActive ) xidIdentMap.put( identifier, startEntry );
                }
                else if ( entry instanceof LogEntry.Commit )
                {
                    LogEntry.Commit commitEntry = (LogEntry.Commit) entry;
                    LogEntry.Start startEntry = startEntriesEncountered.get( identifier );
                    if ( startEntry == null )
                    {
                        // Fetch from log extractor instead (all entries except done records, which will be copied from the source).
                        startEntry = fetchTransactionBulkFromLogExtractor( commitEntry.getTxId(), targetLog );
                        startEntriesEncountered.put( identifier, startEntry );
                    }
                    else
                    {
                        LogExtractor.TxPosition oldPos = positionCache.getStartPosition( commitEntry.getTxId() );
                        LogExtractor.TxPosition newPos = positionCache.cacheStartPosition( commitEntry.getTxId(), startEntry, targetLogVersion );
                        log.logMessage( "Updated tx " + ((LogEntry.Commit) entry ).getTxId() +
                                " from " + oldPos + " to " + newPos );
                    }
                }
                if ( startEntriesEncountered.containsKey( identifier ) )
                {
                    LogIoUtils.writeLogEntry( entry, targetLog );
                }
            }
        }
    }

    private LogEntry.Start fetchTransactionBulkFromLogExtractor( long txId, LogBuffer target ) throws IOException
    {
        LogExtractor extractor = new LogExtractor( positionCache, logLoader, commandFactory, txId, txId );
        InMemoryLogBuffer tempBuffer = new InMemoryLogBuffer();
        extractor.extractNext( tempBuffer );
        ByteBuffer localBuffer = newLogReaderBuffer();
        for ( LogEntry readEntry = null; (readEntry = LogIoUtils.readEntry( localBuffer, tempBuffer, commandFactory )) != null; )
        {
            if ( readEntry instanceof LogEntry.Commit )
            {
                break;
            }
            LogIoUtils.writeLogEntry( readEntry, target );
        }
        return extractor.getLastStartEntry();
    }
}
