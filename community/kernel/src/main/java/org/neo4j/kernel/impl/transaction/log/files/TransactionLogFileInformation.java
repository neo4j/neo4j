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
package org.neo4j.kernel.impl.transaction.log.files;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;

public class TransactionLogFileInformation implements LogFileInformation
{
    private final LogFiles logFiles;
    private final LogHeaderCache logHeaderCache;
    private final TransactionLogFileTimestampMapper logFileTimestampMapper;
    private final TransactionLogFilesContext logFileContext;

    TransactionLogFileInformation( LogFiles logFiles, LogHeaderCache logHeaderCache, TransactionLogFilesContext context )
    {
        this.logFiles = logFiles;
        this.logHeaderCache = logHeaderCache;
        this.logFileContext = context;
        this.logFileTimestampMapper = new TransactionLogFileTimestampMapper( logFiles, context.getLogEntryReader() );
    }

    @Override
    public long getFirstExistingEntryId() throws IOException
    {
        long version = logFiles.getHighestLogVersion();
        long candidateFirstTx = -1;
        while ( logFiles.versionExists( version ) )
        {
            candidateFirstTx = getFirstEntryId( version );
            version--;
        }
        version++; // the loop above goes back one version too far.

        // OK, so we now have the oldest existing log version here. Open it and see if there's any transaction
        // in there. If there is then that transaction is the first one that we have.
        return logFiles.hasAnyEntries( version ) ? candidateFirstTx : -1;
    }

    @Override
    public long getFirstEntryId( long version ) throws IOException
    {
        Long logHeader = logHeaderCache.getLogHeader( version );
        if ( logHeader != null )
        {   // It existed in cache
            return logHeader + 1;
        }

        // Wasn't cached, go look for it
        if ( logFiles.versionExists( version ) )
        {
            long previousVersionLastCommittedTx = logFiles.extractHeader( version ).lastCommittedTxId;
            logHeaderCache.putHeader( version, previousVersionLastCommittedTx );
            return previousVersionLastCommittedTx + 1;
        }
        return -1;
    }

    @Override
    public long getLastEntryId()
    {
        return logFileContext.getLastCommittedTransactionId();
    }

    @Override
    public long getFirstStartRecordTimestamp( long version ) throws IOException
    {
        return logFileTimestampMapper.getTimestampForVersion( version );
    }

    private static class TransactionLogFileTimestampMapper
    {

        private final LogFiles logFiles;
        private final LogEntryReader<ReadableLogChannel> logEntryReader;

        TransactionLogFileTimestampMapper( LogFiles logFiles, LogEntryReader<ReadableLogChannel> logEntryReader )
        {
            this.logFiles = logFiles;
            this.logEntryReader = logEntryReader;
        }

        long getTimestampForVersion( long version ) throws IOException
        {
            LogPosition position = LogPosition.start( version );
            try ( ReadableLogChannel channel = logFiles.getLogFile().getReader( position ) )
            {
                LogEntry entry;
                while ( (entry = logEntryReader.readLogEntry( channel )) != null )
                {
                    if ( entry instanceof LogEntryStart )
                    {
                        return entry.<LogEntryStart>as().getTimeWritten();
                    }
                }
            }
            return -1;
        }
    }
}
