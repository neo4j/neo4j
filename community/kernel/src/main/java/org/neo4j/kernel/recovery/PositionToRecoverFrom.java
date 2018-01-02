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

import org.neo4j.function.ThrowingLongFunction;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

import static org.neo4j.kernel.impl.transaction.log.LogVersionRepository.INITIAL_LOG_VERSION;

/**
 * Utility class to find the log position to start recovery from
 */
public class PositionToRecoverFrom implements ThrowingLongFunction<LogPosition,IOException>
{
    private final LatestCheckPointFinder checkPointFinder;

    public PositionToRecoverFrom( LatestCheckPointFinder checkPointFinder )
    {
        this.checkPointFinder = checkPointFinder;
    }

    /**
     * Find the log position to start recovery from
     *
     * @param currentLogVersion the latest transaction log version
     * @return {@link LogPosition#UNSPECIFIED} if there is no need to recover otherwise the {@link LogPosition} to
     * start recovery from
     * @throws IOException if log files cannot be read
     */
    @Override
    public LogPosition apply( long currentLogVersion ) throws IOException
    {
        LatestCheckPointFinder.LatestCheckPoint latestCheckPoint = checkPointFinder.find( currentLogVersion );
        if ( !latestCheckPoint.commitsAfterCheckPoint )
        {
            return LogPosition.UNSPECIFIED;
        }

        if ( latestCheckPoint.checkPoint != null )
        {
            return latestCheckPoint.checkPoint.getLogPosition();
        }
        else
        {
            if ( latestCheckPoint.oldestLogVersionFound != INITIAL_LOG_VERSION )
            {
                long fromLogVersion = Math.max( INITIAL_LOG_VERSION, latestCheckPoint.oldestLogVersionFound );
                throw new UnderlyingStorageException( "No check point found in any log file from version " +
                                                      fromLogVersion + " to " + currentLogVersion );
            }
            return LogPosition.start( 0 );
        }
    }
}
