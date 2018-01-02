/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacylogs;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.function.Function;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableVersionableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;

import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_VERSION;

class LegacyLogEntryReader
{
    private final FileSystemAbstraction fs;
    private final Function<LogHeader,LogEntryReader<ReadableVersionableLogChannel>> readerFactory;

    LegacyLogEntryReader( FileSystemAbstraction fs,
            Function<LogHeader,LogEntryReader<ReadableVersionableLogChannel>> readerFactory )
    {
        this.fs = fs;
        this.readerFactory = readerFactory;
    }

    LegacyLogEntryReader( FileSystemAbstraction fs )
    {
        this( fs, new Function<LogHeader,LogEntryReader<ReadableVersionableLogChannel>>()
        {
            @Override
            public LogEntryReader<ReadableVersionableLogChannel> apply( LogHeader from ) throws RuntimeException
            {
                return new VersionAwareLogEntryReader<>( from.logFormatVersion );
            }
        } );
    }

    public Pair<LogHeader, IOCursor<LogEntry>> openReadableChannel( File logFile ) throws IOException
    {
        final StoreChannel rawChannel = fs.open( logFile, "r" );
        final ByteBuffer buffer = ByteBuffer.allocate( LOG_HEADER_SIZE );

        final LogHeader header = readLogHeader( buffer, rawChannel, false );
        LogEntryReader<ReadableVersionableLogChannel> reader = readerFactory.apply( header );

        // this ensures that the last committed txId field in the header is initialized properly
        long lastCommittedTxId = Math.max( BASE_TX_ID, header.lastCommittedTxId );

        final PhysicalLogVersionedStoreChannel channel =
                new PhysicalLogVersionedStoreChannel( rawChannel, header.logVersion, header.logFormatVersion );
        final ReadableVersionableLogChannel readableChannel =
                new ReadAheadLogChannel( channel, NO_MORE_CHANNELS, DEFAULT_READ_AHEAD_SIZE );
        final IOCursor<LogEntry> cursor = new LogEntrySortingCursor( reader, readableChannel );

        return Pair.of( new LogHeader( CURRENT_LOG_VERSION, header.logVersion, lastCommittedTxId ), cursor );
    }
}
