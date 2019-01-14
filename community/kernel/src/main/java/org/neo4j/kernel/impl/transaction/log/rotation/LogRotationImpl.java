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
package org.neo4j.kernel.impl.transaction.log.rotation;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.internal.DatabaseHealth;

/**
 * Default implementation of the LogRotation interface.
 */
public class LogRotationImpl implements LogRotation
{
    private final LogRotation.Monitor monitor;
    private final LogFiles logFiles;
    private final DatabaseHealth databaseHealth;
    private final LogFile logFile;

    public LogRotationImpl( Monitor monitor, LogFiles logFiles, DatabaseHealth databaseHealth )
    {
        this.monitor = monitor;
        this.logFiles = logFiles;
        this.databaseHealth = databaseHealth;
        this.logFile = logFiles.getLogFile();
    }

    @Override
    public boolean rotateLogIfNeeded( LogAppendEvent logAppendEvent ) throws IOException
    {
        /* We synchronize on the writer because we want to have a monitor that another thread
         * doing force (think batching of writes), such that it can't see a bad state of the writer
         * even when rotating underlying channels.
         */
        if ( logFile.rotationNeeded() )
        {
            synchronized ( logFile )
            {
                if ( logFile.rotationNeeded() )
                {
                    try ( LogRotateEvent rotateEvent = logAppendEvent.beginLogRotate() )
                    {
                        doRotate();
                    }
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * use for test purpose only
     * @throws IOException
     */
    @Override
    public void rotateLogFile() throws IOException
    {
        synchronized ( logFile )
        {
            doRotate();
        }
    }

    private void doRotate() throws IOException
    {
        long currentVersion = logFiles.getHighestLogVersion();
        /*
         * In order to rotate the current log file safely we need to assert that the kernel is still
         * at full health. In case of a panic this rotation will be aborted, which is the safest alternative.
         */
        databaseHealth.assertHealthy( IOException.class );
        monitor.startedRotating( currentVersion );
        logFile.rotate();
        monitor.finishedRotating( currentVersion );
    }
}
