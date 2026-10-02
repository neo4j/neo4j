/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.wal.rotation;

import java.io.IOException;
import java.time.Clock;
import java.util.function.ToLongFunction;
import org.neo4j.monitoring.Panic;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.RotatableFile;
import org.neo4j.wal.files.checkpoint.CheckpointLogFile;
import org.neo4j.wal.rotation.monitor.LogRotationMonitor;

/**
 * Default implementation of the LogRotation interface.
 */
public class FileLogRotation implements LogRotation {
    private final Clock clock;
    private final LogRotationMonitor monitor;
    private final LogRotationMonitor.LogType type;
    private final Panic databasePanic;
    private final RotatableFile rotatableFile;
    private long lastRotationCompleted;
    private final ToLongFunction<RotatableFile.RotationInfo> lastAppendIndexOfRotation;

    public static LogRotation checkpointLogRotation(
            CheckpointLogFile checkpointLogFile,
            LogFile logFile,
            Clock clock,
            Panic databasePanic,
            LogRotationMonitor monitor) {
        return new FileLogRotation(
                checkpointLogFile,
                LogRotationMonitor.LogType.CHECKPOINT,
                clock,
                databasePanic,
                monitor,
                // Checkpoint headers carry no append index, so report the transaction log's instead
                rotationInfo -> logFile.getLastEntryAppendIndexInLogFiles());
    }

    public static LogRotation transactionLogRotation(
            LogFile logFile, Clock clock, Panic databasePanic, LogRotationMonitor monitor) {
        return new FileLogRotation(
                logFile,
                LogRotationMonitor.LogType.TRANSACTIONS,
                clock,
                databasePanic,
                monitor,
                rotationInfo -> rotationInfo.nextHeader().lastAppendIndex());
    }

    private FileLogRotation(
            RotatableFile rotatableFile,
            LogRotationMonitor.LogType type,
            Clock clock,
            Panic databasePanic,
            LogRotationMonitor monitor,
            ToLongFunction<RotatableFile.RotationInfo> lastAppendIndexOfRotation) {
        this.clock = clock;
        this.monitor = monitor;
        this.type = type;
        this.databasePanic = databasePanic;
        this.rotatableFile = rotatableFile;
        this.lastAppendIndexOfRotation = lastAppendIndexOfRotation;
    }

    @Override
    public boolean rotateLogIfNeeded(LogRotateEvents logRotateEvents) throws IOException {
        /* We synchronize on the writer because we want to have a monitor that another thread
         * doing force (think batching of writes), such that it can't see a bad state of the writer
         * even when rotating underlying channels.
         */
        if (rotatableFile.rotationNeeded()) {
            synchronized (rotatableFile) {
                return locklessRotateLogIfNeeded(logRotateEvents, NextLogHeader.builder());
            }
        }
        return false;
    }

    @Override
    public boolean locklessRotateLogIfNeeded(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder)
            throws IOException {
        if (rotatableFile.rotationNeeded()) {
            doRotate(logRotateEvents, headerBuilder);
            return true;
        }
        return false;
    }

    @Override
    public boolean locklessBatchedRotateLogIfNeeded(
            LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder) throws IOException {
        return locklessRotateLogIfNeeded(logRotateEvents, headerBuilder);
    }

    @Override
    public void rotateLogFile(LogRotateEvents logRotateEvents) throws IOException {
        synchronized (rotatableFile) {
            doRotate(logRotateEvents, NextLogHeader.builder());
        }
    }

    @Override
    public void locklessRotateLogFile(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder)
            throws IOException {
        doRotate(logRotateEvents, headerBuilder);
    }

    @Override
    public long rotationSize() {
        return rotatableFile.rotationSize();
    }

    /**
     * Callers must hold exclusive access to the file, so the active file version can't move before the rotation.
     */
    private void doRotate(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder) throws IOException {
        try (LogRotateEvent rotateEvent = logRotateEvents.beginLogRotate()) {
            long currentVersion = rotatableFile.activeFileVersion();
            /*
             * In order to rotate the log file safely we need to assert that the kernel is still
             * at full health. In case of a panic this rotation will be aborted, which is the safest alternative.
             */
            databasePanic.assertNoPanic(IOException.class);
            long startTimeMillis = clock.millis();
            monitor.startRotation(type, currentVersion);
            RotatableFile.RotationInfo logFileInfo = rotatableFile.rotate(headerBuilder);
            long millisSinceLastRotation = lastRotationCompleted == 0 ? 0 : startTimeMillis - lastRotationCompleted;
            lastRotationCompleted = clock.millis();
            long rotationElapsedTime = lastRotationCompleted - startTimeMillis;
            rotateEvent.rotationCompleted(rotationElapsedTime);
            monitor.finishLogRotation(
                    logFileInfo.file(),
                    type,
                    currentVersion,
                    logFileInfo.logHeader(),
                    lastAppendIndexOfRotation.applyAsLong(logFileInfo),
                    rotationElapsedTime,
                    millisSinceLastRotation);
        }
    }
}
