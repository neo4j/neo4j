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
package org.neo4j.kernel.recovery;

import java.io.IOException;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailScanner;

import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;

/**
 * Utility class to find the log position to start recovery from
 */
public class PositionToRecoverFrom implements ThrowingSupplier<LogPosition,IOException>
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
    }

    public static final Monitor NO_MONITOR = new Monitor()
    {
    };

    private final LogTailScanner logTailScanner;
    private final Monitor monitor;

    public PositionToRecoverFrom( LogTailScanner logTailScanner, Monitor monitor )
    {
        this.logTailScanner = logTailScanner;
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
    public LogPosition get() throws IOException
    {
        LogTailScanner.LogTailInformation logTailInformation = logTailScanner.getTailInformation();
        if ( !logTailInformation.commitsAfterLastCheckPoint )
        {
            monitor.noCommitsAfterLastCheckPoint(
                    logTailInformation.lastCheckPoint != null ? logTailInformation.lastCheckPoint.getLogPosition() : null );
            return LogPosition.UNSPECIFIED;
        }

        if ( logTailInformation.lastCheckPoint != null )
        {
            monitor.commitsAfterLastCheckPoint( logTailInformation.lastCheckPoint.getLogPosition(),
                    logTailInformation.firstTxIdAfterLastCheckPoint );
            return logTailInformation.lastCheckPoint.getLogPosition();
        }
        else
        {
            if ( logTailInformation.oldestLogVersionFound != INITIAL_LOG_VERSION )
            {
                long fromLogVersion = Math.max( INITIAL_LOG_VERSION, logTailInformation.oldestLogVersionFound );
                throw new UnderlyingStorageException( "No check point found in any log file from version " +
                                                      fromLogVersion + " to " + logTailInformation.currentLogVersion );
            }
            monitor.noCheckPointFound();
            return LogPosition.start( 0 );
        }
    }
}
