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
package org.neo4j.kernel.impl.transaction.log.rotation;

import java.io.IOException;

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;

/**
 * Default implementation of the LogRotation interface.
 */
public class LogRotationImpl implements LogRotation
{
    private final LogRotation.Monitor monitor;
    private final LogFile logFile;
    private final KernelHealth kernelHealth;

    public LogRotationImpl( Monitor monitor, LogFile logFile, KernelHealth kernelHealth )
    {
        this.monitor = monitor;
        this.logFile = logFile;
        this.kernelHealth = kernelHealth;
    }

    @Override
    public boolean rotateLogIfNeeded( LogAppendEvent logAppendEvent ) throws IOException
    {
        /* We synchronize on the writer because we want to have a monitor that another thread
         * doing force (think batching of writes), such that it can't see a bad state of the writer
         * even when rotating underlying channels.
         */
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
            return false;
        }
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
        long currentVersion = logFile.currentLogVersion();
        /*
         * In order to rotate the current log file safely we need to assert that the kernel is still
         * at full health. In case of a panic this rotation will be aborted, which is the safest alternative.
         */
        kernelHealth.assertHealthy( IOException.class );
        monitor.startedRotating( currentVersion );
        logFile.rotate();
        monitor.finishedRotating( currentVersion );
    }
}
