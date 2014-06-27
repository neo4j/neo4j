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

import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;

public class PhysicalLogFileInformation implements LogFileInformation
{
    private final PhysicalLogFiles logFiles;
    private final TransactionMetadataCache transactionMetadataCache;
    private final TransactionIdStore transactionIdStore;

    public PhysicalLogFileInformation( PhysicalLogFiles logFiles,
            TransactionMetadataCache transactionMetadataCache, TransactionIdStore transactionIdStore )
    {
        this.logFiles = logFiles;
        this.transactionMetadataCache = transactionMetadataCache;
        this.transactionIdStore = transactionIdStore;
    }

    @Override
    public Long getFirstCommittedTxId() throws IOException
    {
        long version = logFiles.getHighestLogVersion();
        Long firstCommittedTx = null;
        while ( logFiles.versionExists( version ) )
        {
            firstCommittedTx = getOrExtractFirstCommittedTx( version );
            version--;
        }
        return firstCommittedTx;
    }

    @Override
    public Long getFirstCommittedTxId( long version ) throws IOException
    {
        if ( version == 0 )
        {
            return 1L;
        }

        // First committed tx for version V = last committed tx version V-1 + 1
        return getOrExtractFirstCommittedTx( version );
    }

    private Long getOrExtractFirstCommittedTx( long version ) throws IOException
    {
        Long header = transactionMetadataCache.getHeader( version );
        if ( header != null )
        {   // It existed in cache
            return header+1;
        }

        // Wasn't cached, go look for it
        if ( logFiles.versionExists( version ) )
        {
            long[] headerLongs = logFiles.extractHeader( version );
            long previousVersionLastCommittedTx = headerLongs[1];
            transactionMetadataCache.putHeader( version, previousVersionLastCommittedTx );
            return previousVersionLastCommittedTx+1;
        }
        return null;
    }

    @Override
    public long getLastCommittedTxId()
    {
        return transactionIdStore.getLastCommittingTransactionId();
    }

    @Override
    public Long getFirstStartRecordTimestamp( long version ) throws IOException
    {
//        ReadableByteChannel log = null;
//        try
//        {
//            ByteBuffer buffer = LogExtractor.newLogReaderBuffer();
//            log = getLogicalLog( version );
//            VersionAwareLogEntryReader.readLogHeader( buffer, log, true );
//            LogDeserializer deserializer = new LogDeserializer( buffer, commandReaderFactory );
//
//            TimeWrittenConsumer consumer = new TimeWrittenConsumer();
//
//            try ( Cursor<LogEntry, IOException> cursor = deserializer.cursor( log ) )
//            {
//                while( cursor.next( consumer ) );
//            }
//            return consumer.getTimeWritten();
//        }
//        finally
//        {
//            if ( log != null )
//            {
//                log.close();
//            }
//        }

        return null;
    }
}
