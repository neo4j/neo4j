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
package org.neo4j.kernel.recovery;

import java.io.IOException;

import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;

import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.recovery.RecoveryStartInformation.MISSING_LOGS;
import static org.neo4j.kernel.recovery.RecoveryStartInformation.NO_RECOVERY_REQUIRED;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;

/**
 * Utility class to find the log position to start recovery from
 */
public class RecoveryStartInformationProvider implements ThrowingSupplier<RecoveryStartInformation,IOException>
{
    public interface Monitor
    {
        /**
         * There's a check point log entry as the last entry in the transaction log.
         *
         * @param logPosition {@link LogPosition} of the last check point.
         */
        default void noCommitsAfterLastCheckPoint( LogPosition logPosition )
        {   // no-op by default
        }

        /**
         * There's a check point log entry, but there are other log entries after it.
         *
         * @param logPosition {@link LogPosition} pointing to the first log entry after the last
         * check pointed transaction.
         * @param firstTxIdAfterLastCheckPoint transaction id of the first transaction after the last check point.
         */
        default void commitsAfterLastCheckPoint( LogPosition logPosition, long firstTxIdAfterLastCheckPoint )
        {   // no-op by default
        }

        /**
         * No check point log entry found in the transaction log.
         */
        default void noCheckPointFound()
        {   // no-op by default
        }

        /**
         * Failure to read initial header of initial log file
         */
        default void failToExtractInitialFileHeader( Exception e )
        {
            // no-op by default
        }
    }

    public static final Monitor NO_MONITOR = new Monitor()
    {
    };

    private final LogTailScanner logTailScanner;
    private final LogFiles logFiles;
    private final Monitor monitor;

    public RecoveryStartInformationProvider( LogTailScanner logTailScanner, LogFiles logFiles, Monitor monitor )
    {
        this.logTailScanner = logTailScanner;
        this.logFiles = logFiles;
        this.monitor = monitor;
    }

    /**
     * Find the log position to start recovery from
     *
     * @return {@link LogPosition#UNSPECIFIED} if there is no need to recover otherwise the {@link LogPosition} to
     * start recovery from
     * @throws IOException if log files cannot be read
     */
    @Override
    public RecoveryStartInformation get()
    {
        LogTailScanner.LogTailInformation logTailInformation = logTailScanner.getTailInformation();
        CheckPoint lastCheckPoint = logTailInformation.lastCheckPoint;
        long txIdAfterLastCheckPoint = logTailInformation.firstTxIdAfterLastCheckPoint;

        if ( !logTailInformation.isRecoveryRequired() )
        {
            monitor.noCommitsAfterLastCheckPoint( lastCheckPoint != null ? lastCheckPoint.getLogPosition() : null );
            return NO_RECOVERY_REQUIRED;
        }
        if ( logTailInformation.logsMissing() )
        {
            return MISSING_LOGS;
        }
        if ( logTailInformation.commitsAfterLastCheckpoint() )
        {
            if ( lastCheckPoint == null )
            {
                if ( logTailInformation.oldestLogVersionFound != INITIAL_LOG_VERSION )
                {
                    long fromLogVersion = Math.max( INITIAL_LOG_VERSION, logTailInformation.oldestLogVersionFound );
                    throw new UnderlyingStorageException(
                            "No check point found in any log file from version " + fromLogVersion + " to " + logTailInformation.currentLogVersion );
                }
                monitor.noCheckPointFound();
                LogPosition position = tryExtractHeaderSize();
                return createRecoveryInformation( position, txIdAfterLastCheckPoint );
            }
            LogPosition checkpointLogPosition = lastCheckPoint.getLogPosition();
            monitor.commitsAfterLastCheckPoint( checkpointLogPosition, txIdAfterLastCheckPoint );
            return createRecoveryInformation( lastCheckPoint.getLogPosition(), txIdAfterLastCheckPoint );
        }
        else
        {
            throw new UnderlyingStorageException( "Fail to determine recovery information Log tail info: " + logTailInformation );
        }
    }

    private LogPosition tryExtractHeaderSize()
    {
        try
        {
            return logFiles.extractHeader( 0 ).getStartPosition();
        }
        catch ( IOException e )
        {
            monitor.failToExtractInitialFileHeader( e );
            // we can't event read header, lets assume we need to recover from the most latest format and from the beginning
            return new LogPosition( 0, CURRENT_FORMAT_LOG_HEADER_SIZE );
        }
    }

    private static RecoveryStartInformation createRecoveryInformation( LogPosition logPosition, long firstTxId )
    {
        return new RecoveryStartInformation( logPosition, firstTxId );
    }
}
