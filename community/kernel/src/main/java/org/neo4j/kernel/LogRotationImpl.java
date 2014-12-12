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
package org.neo4j.kernel;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogRotation;
import org.neo4j.kernel.impl.transaction.log.LogRotationControl;

/**
 * Default implementation of the LogRotation interface.
 */
public class LogRotationImpl
    implements LogRotation
{
    private LogRotation.Monitor monitor;
    private LogFile logFile;
    private LogRotationControl logRotationControl;

    public LogRotationImpl( Monitor monitor, LogFile logFile,
            LogRotationControl logRotationControl )
    {
        this.monitor = monitor;
        this.logFile = logFile;
        this.logRotationControl = logRotationControl;
    }

    @Override
    public boolean rotateLogIfNeeded() throws IOException
    {
        if ( logFile.rotationNeeded() )
        {
            /* We synchronize on the writer because we want to have a monitor that another thread
             * doing force (think batching of writes), such that it can't see a bad state of the writer
             * even when rotating underlying channels.
             */
            boolean rotate;
            synchronized ( logFile )
            {
                if ( rotate = logFile.rotationNeeded() )
                {
                    /*
                     * First we flush the store. If we fail now or during the flush, on recovery we'll discover
                     * the current log file and replay it. Everything will be ok.
                     */
                    logRotationControl.awaitAllTransactionsClosed();
                    logRotationControl.forceEverything();

                    logFile.rotate();
                }
            }

            if ( rotate )
            {
                monitor.rotatedLog();
            }

            return rotate;
        }
        else
        {
            return false;
        }
    }

    @Override
    public void rotateLogFile() throws IOException
    {
        synchronized ( logFile )
        {
            /*
             * First we flush the store. If we fail now or during the flush, on recovery we'll discover
             * the current log file and replay it. Everything will be ok.
             */
            logRotationControl.awaitAllTransactionsClosed();
            logRotationControl.forceEverything();

            logFile.rotate();
        }

        monitor.rotatedLog();

    }
}
