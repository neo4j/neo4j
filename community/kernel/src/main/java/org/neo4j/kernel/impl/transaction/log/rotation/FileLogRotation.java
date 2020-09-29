/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.nio.file.Path;
import java.time.Clock;
import java.util.function.LongSupplier;

import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.RotatableFile;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointLogFile;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvents;
import org.neo4j.monitoring.Health;
import org.neo4j.util.VisibleForTesting;

/**
 * Default implementation of the LogRotation interface.
 */
public class FileLogRotation implements LogRotation
{
    private final Clock clock;
    private final LogRotationMonitor monitor;
    private final Health databaseHealth;
    private final RotatableFile rotatableFile;
    private long lastRotationCompleted;
    private final LongSupplier lastTransactionIdSupplier;
    private final LongSupplier fileVersionSupplier;

    public static LogRotation checkpointLogRotation( CheckpointLogFile checkpointLogFile, LogFile logFile, Clock clock, Health databaseHealth,
            LogRotationMonitor monitor )
    {
        return new FileLogRotation( checkpointLogFile, clock, databaseHealth, monitor,
                () -> logFile.getLogFileInformation().committingEntryId(), checkpointLogFile::getCurrentDetachedLogVersion );
    }

    public static LogRotation transactionLogRotation( LogFiles logFiles, Clock clock, Health databaseHealth, LogRotationMonitor monitor )
    {
        return new FileLogRotation( logFiles.getLogFile(), clock, databaseHealth, monitor,
                () -> logFiles.getLogFile().getLogFileInformation().committingEntryId(), logFiles.getLogFile()::getHighestLogVersion );
    }

    private FileLogRotation( RotatableFile rotatableFile, Clock clock, Health databaseHealth, LogRotationMonitor monitor,
            LongSupplier lastTransactionIdSupplier, LongSupplier fileVersionSupplier )
    {
        this.clock = clock;
        this.monitor = monitor;
        this.databaseHealth = databaseHealth;
        this.rotatableFile = rotatableFile;
        this.lastTransactionIdSupplier = lastTransactionIdSupplier;
        this.fileVersionSupplier = fileVersionSupplier;
    }

    @Override
    public boolean rotateLogIfNeeded( LogRotateEvents logRotateEvents ) throws IOException
    {
        /* We synchronize on the writer because we want to have a monitor that another thread
         * doing force (think batching of writes), such that it can't see a bad state of the writer
         * even when rotating underlying channels.
         */
        if ( rotatableFile.rotationNeeded() )
        {
            synchronized ( rotatableFile )
            {
                if ( rotatableFile.rotationNeeded() )
                {
                    doRotate( logRotateEvents );
                    return true;
                }
            }
        }
        return false;
    }

    @VisibleForTesting
    @Override
    public void rotateLogFile( LogRotateEvents logRotateEvents ) throws IOException
    {
        synchronized ( rotatableFile )
        {
            doRotate( logRotateEvents );
        }
    }

    private void doRotate( LogRotateEvents logRotateEvents ) throws IOException
    {
        try ( LogRotateEvent rotateEvent = logRotateEvents.beginLogRotate() )
        {
            long currentVersion = fileVersionSupplier.getAsLong();
            /*
             * In order to rotate the log file safely we need to assert that the kernel is still
             * at full health. In case of a panic this rotation will be aborted, which is the safest alternative.
             */
            databaseHealth.assertHealthy( IOException.class );
            long startTimeMillis = clock.millis();
            monitor.startRotation( currentVersion );
            Path newLogFile = rotatableFile.rotate();
            long lastTransactionId = lastTransactionIdSupplier.getAsLong();
            long millisSinceLastRotation = lastRotationCompleted == 0 ? 0 : startTimeMillis - lastRotationCompleted;
            lastRotationCompleted = clock.millis();
            long rotationElapsedTime = lastRotationCompleted - startTimeMillis;
            rotateEvent.rotationCompleted( rotationElapsedTime );
            monitor.finishLogRotation( newLogFile, currentVersion, lastTransactionId, rotationElapsedTime, millisSinceLastRotation );
        }
    }
}
