/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Path;
import java.util.List;
import java.util.ListIterator;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogTailInformation;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.storageengine.api.StoreId;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.files.RangeLogVersionVisitor.UNKNOWN;

public class DetachedLogTailScanner extends AbstractLogTailScanner
{
    private final CheckpointFile checkPointFile;
    private final boolean failOnCorruptedLogFiles;
    private final FileSystemAbstraction fileSystem;

    public DetachedLogTailScanner( LogFiles logFiles, TransactionLogFilesContext context, CheckpointFile checkpointFile )
    {
        super( logFiles, context.getLogEntryReader(), context.getMonitors(), context.getLogProvider(), context.getMemoryTracker() );
        this.checkPointFile = checkpointFile;
        this.fileSystem = context.getFileSystem();
        this.failOnCorruptedLogFiles = context.isFailOnCorruptedLogFiles();
    }

    @Override
    protected LogTailInformation findLogTail()
    {
        LogFile logFile = logFiles.getLogFile();
        long highestLogVersion = logFile.getHighestLogVersion();
        long lowestLogVersion = logFile.getLowestLogVersion();
        try
        {
            var lastAccessibleCheckpoint = checkPointFile.findLatestCheckpoint();
            if ( lastAccessibleCheckpoint.isEmpty() )
            {
                return noCheckpointLogTail( logFile, highestLogVersion, lowestLogVersion );
            }
            var checkpoint = lastAccessibleCheckpoint.get();
            //found checkpoint pointing to existing position in existing log file
            if ( isValidCheckpoint( logFile, checkpoint ) )
            {
                return validCheckpointLogTail( logFile, highestLogVersion, lowestLogVersion, checkpoint );
            }
            if ( failOnCorruptedLogFiles )
            {
                var exceptionMessage = format( "Last available %s checkpoint does not point to a valid location in transaction logs.", checkpoint );
                throwUnableToCleanRecover( new RuntimeException( exceptionMessage ) );
            }
            // our last checkpoint is not valid (we have a pointer to non existent place) lets try to find last one that looks correct
            List<CheckpointInfo> checkpointInfos = checkPointFile.reachableCheckpoints();
            // we know that last one is not valid so no reason to double check that again
            ListIterator<CheckpointInfo> reverseCheckpoints = checkpointInfos.listIterator( checkpointInfos.size() - 1 );
            while ( reverseCheckpoints.hasPrevious() )
            {
                CheckpointInfo previousCheckpoint = reverseCheckpoints.previous();
                if ( isValidCheckpoint( logFile, previousCheckpoint ) )
                {
                    return validCheckpointLogTail( logFile, highestLogVersion, lowestLogVersion, previousCheckpoint );
                }
            }
            // we did not found any valid, we need to restore from the start if possible
            return noCheckpointLogTail( logFile, highestLogVersion, lowestLogVersion );
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( t );
        }
    }

    private LogTailInformation validCheckpointLogTail( LogFile logFile, long highestLogVersion, long lowestLogVersion, CheckpointInfo checkpoint )
            throws IOException
    {
        var entries = getFirstTransactionIdAfterCheckpoint( logFile, checkpoint.getTransactionLogPosition() );
        return new LogTailInformation( checkpoint, entries.isPresent(), entries.getCommitId(), lowestLogVersion == UNKNOWN, highestLogVersion,
                entries.getEntryVersion(), checkpoint.storeId() );
    }

    private LogTailInformation noCheckpointLogTail( LogFile logFile, long highestLogVersion, long lowestLogVersion ) throws IOException
    {
        var entries = getFirstTransactionId( logFile, lowestLogVersion );
        return new LogTailInformation( entries.isPresent(),
                entries.getCommitId(), lowestLogVersion == UNKNOWN, highestLogVersion, entries.getEntryVersion() );
    }

    private StartCommitEntries getFirstTransactionId( LogFile logFile, long lowestLogVersion ) throws IOException
    {
        var logPosition = logFile.versionExists( lowestLogVersion ) ? logFile.extractHeader( lowestLogVersion ).getStartPosition()
                                                                    : new LogPosition( lowestLogVersion, CURRENT_FORMAT_LOG_HEADER_SIZE );
        return getFirstTransactionIdAfterCheckpoint( logFile, logPosition );
    }

    /**
     * Valid checkpoint points to valid location in a file, which exists and header store id matches with checkpoint store id.
     * Otherwise checkpoint is not considered valid and we need to recover.
     */
    private boolean isValidCheckpoint( LogFile logFile, CheckpointInfo checkpointInfo ) throws IOException
    {
        LogPosition logPosition = checkpointInfo.getTransactionLogPosition();
        long logVersion = logPosition.getLogVersion();
        if ( !logFile.versionExists( logVersion ) )
        {
            return false;
        }
        Path logFileForVersion = logFile.getLogFileForVersion( logVersion );
        if ( fileSystem.getFileSize( logFileForVersion ) < logPosition.getByteOffset() )
        {
            return false;
        }
        LogHeader logHeader = logFile.extractHeader( logVersion );
        StoreId headerStoreId = logHeader.getStoreId();
        return StoreId.UNKNOWN.equals( headerStoreId ) || headerStoreId.equalsIgnoringVersion( checkpointInfo.storeId() );
    }

    private StartCommitEntries getFirstTransactionIdAfterCheckpoint( LogFile logFile, LogPosition logPosition ) throws IOException
    {
        boolean corruptedTransactionLogs = false;
        LogEntryStart start = null;
        LogEntryCommit commit = null;
        LogPosition lookupPosition = null;
        long logVersion = logPosition.getLogVersion();
        try
        {
            while ( logFile.versionExists( logVersion ) )
            {
                lookupPosition = lookupPosition == null ? logPosition : logFile.extractHeader( logVersion ).getStartPosition();

                try ( var reader = logFile.getReader( lookupPosition, NO_MORE_CHANNELS );
                      var cursor = new LogEntryCursor( logEntryReader, reader ) )
                {
                    LogEntry entry;
                    while ( (start == null || commit == null) && cursor.next() )
                    {
                        entry = cursor.get();
                        if ( commit == null && entry instanceof LogEntryCommit )
                        {
                            commit = (LogEntryCommit) entry;
                        }
                        else if ( start == null && entry instanceof LogEntryStart )
                        {
                            start = (LogEntryStart) entry;
                        }
                    }
                }
                if ( (start != null) && (commit != null) )
                {
                    return new StartCommitEntries( start, commit );
                }
                verifyReaderPosition( logVersion, logEntryReader.lastPosition() );
                logVersion++;
            }
        }
        catch ( Error | ClosedByInterruptException e )
        {
            // These should not be parsing errors
            throw e;
        }
        catch ( Throwable t )
        {
            monitor.corruptedLogFile( logVersion, t );
            if ( failOnCorruptedLogFiles )
            {
                throwUnableToCleanRecover( t );
            }
            corruptedTransactionLogs = true;
        }
        return new StartCommitEntries( start, commit, corruptedTransactionLogs );
    }

    private static class StartCommitEntries
    {
        private final LogEntryStart start;
        private final LogEntryCommit commit;
        private final boolean corruptedLogs;

        StartCommitEntries( LogEntryStart start, LogEntryCommit commit )
        {
            this( start, commit, false );
        }

        StartCommitEntries( LogEntryStart start, LogEntryCommit commit, boolean corruptedLogs )
        {
            this.start = start;
            this.commit = commit;
            this.corruptedLogs = corruptedLogs;
        }

        public long getCommitId()
        {
            return commit != null ? commit.getTxId() : NO_TRANSACTION_ID;
        }

        public boolean isPresent()
        {
            return start != null || commit != null || corruptedLogs;
        }

        public byte getEntryVersion()
        {
            if ( start != null )
            {
                return start.getVersion().version();
            }
            if ( commit != null )
            {
                return commit.getVersion().version();
            }
            return 0;
        }
    }
}
