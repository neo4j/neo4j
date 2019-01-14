/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.tools.dump;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.tools.dump.log.TransactionLogEntryCursor;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes.CHECK_POINT;
import static org.neo4j.tools.util.TransactionLogUtils.openVersionedChannel;

/**
 * Merely a utility which, given a store directory or log file, reads the transaction log(s) as a stream of transactions
 * and invokes methods on {@link Monitor}.
 */
public class TransactionLogAnalyzer
{
    /**
     * Receiving call-backs for all kinds of different events while analyzing the stream of transactions.
     */
    public interface Monitor
    {
        /**
         * Called when transitioning to a new log file, crossing a log version bridge. This is also called for the
         * first log file opened.
         *
         * @param file {@link File} pointing to the opened log file.
         * @param logVersion log version.
         */
        default void logFile( File file, long logVersion ) throws IOException
        {   // no-op by default
        }

        default void endLogFile()
        {
        }

        /**
         * A complete transaction with {@link LogEntryStart}, one or more {@link LogEntryCommand} and {@link LogEntryCommit}.
         *
         * @param transactionEntries the log entries making up the transaction, including start/commit entries.
         */
        default void transaction( LogEntry[] transactionEntries )
        {   // no-op by default
        }

        /**
         * {@link CheckPoint} log entry in between transactions.
         *
         * @param checkpoint the {@link CheckPoint} log entry.
         * @param checkpointEntryPosition {@link LogPosition} of the checkpoint entry itself.
         */
        default void checkpoint( CheckPoint checkpoint, LogPosition checkpointEntryPosition )
        {   // no-op by default
        }
    }

    public static Monitor all( Monitor... monitors )
    {
        return new CombinedMonitor( monitors );
    }

    /**
     * Analyzes transactions found in log file(s) specified by {@code storeDirOrLogFile} calling methods on the supplied
     * {@link Monitor} for each encountered data item.
     *
     * @param fileSystem {@link FileSystemAbstraction} to find the files on.
     * @param storeDirOrLogFile {@link File} pointing either to a directory containing transaction log files, or directly
     * pointing to a single transaction log file to analyze.
     * @param invalidLogEntryHandler {@link InvalidLogEntryHandler} to pass in to the internal {@link LogEntryReader}.
     * @param monitor {@link Monitor} receiving call-backs for all {@link Monitor#transaction(LogEntry[]) transactions},
     * {@link Monitor#checkpoint(CheckPoint, LogPosition) checkpoints} and {@link Monitor#logFile(File, long) log file transitions}
     * encountered during the analysis.
     * @throws IOException on I/O error.
     */
    public static void analyze( FileSystemAbstraction fileSystem, File storeDirOrLogFile,
            InvalidLogEntryHandler invalidLogEntryHandler, Monitor monitor )
            throws IOException
    {
        File firstFile;
        LogVersionBridge bridge;
        ReadAheadLogChannel channel;
        LogEntryReader<ReadableClosablePositionAwareChannel> entryReader;
        LogPositionMarker positionMarker;
        if ( storeDirOrLogFile.isDirectory() )
        {
            // Use natural log version bridging if a directory is supplied
            final LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDirOrLogFile, fileSystem ).build();
            bridge = new ReaderLogVersionBridge( logFiles )
            {
                @Override
                public LogVersionedStoreChannel next( LogVersionedStoreChannel channel ) throws IOException
                {
                    LogVersionedStoreChannel next = super.next( channel );
                    if ( next != channel )
                    {
                        monitor.endLogFile();
                        monitor.logFile( logFiles.getLogFileForVersion( next.getVersion() ), next.getVersion() );
                    }
                    return next;
                }
            };
            long lowestLogVersion = logFiles.getLowestLogVersion();
            if ( lowestLogVersion < 0 )
            {
                throw new IllegalStateException( format( "Transaction logs at '%s' not found.", storeDirOrLogFile ) );
            }
            firstFile = logFiles.getLogFileForVersion( lowestLogVersion );
            monitor.logFile( firstFile, lowestLogVersion );
        }
        else
        {
            // Use no bridging, simply reading this single log file if a file is supplied
            firstFile = storeDirOrLogFile;
            final LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDirOrLogFile, fileSystem ).build();
            monitor.logFile( firstFile, logFiles.getLogVersion( firstFile ) );
            bridge = NO_MORE_CHANNELS;
        }

        channel = new ReadAheadLogChannel( openVersionedChannel( fileSystem, firstFile ), bridge );
        entryReader =
                new VersionAwareLogEntryReader<>( new RecordStorageCommandReaderFactory(), invalidLogEntryHandler );
        positionMarker = new LogPositionMarker();
        try ( TransactionLogEntryCursor cursor = new TransactionLogEntryCursor( new LogEntryCursor( entryReader, channel ) ) )
        {
            channel.getCurrentPosition( positionMarker );
            while ( cursor.next() )
            {
                LogEntry[] tx = cursor.get();
                if ( tx.length == 1 && tx[0].getType() == CHECK_POINT )
                {
                    monitor.checkpoint( tx[0].as(), positionMarker.newPosition() );
                }
                else
                {
                    monitor.transaction( tx );
                }
            }
        }
        monitor.endLogFile();
    }

    private static class CombinedMonitor implements Monitor
    {
        private final Monitor[] monitors;

        CombinedMonitor( Monitor[] monitors )
        {
            this.monitors = monitors;
        }

        @Override
        public void logFile( File file, long logVersion ) throws IOException
        {
            for ( Monitor monitor : monitors )
            {
                monitor.logFile( file, logVersion );
            }
        }

        @Override
        public void transaction( LogEntry[] transactionEntries )
        {
            for ( Monitor monitor : monitors )
            {
                monitor.transaction( transactionEntries );
            }
        }

        @Override
        public void checkpoint( CheckPoint checkpoint, LogPosition checkpointEntryPosition )
        {
            for ( Monitor monitor : monitors )
            {
                monitor.checkpoint( checkpoint, checkpointEntryPosition );
            }
        }
    }
}
