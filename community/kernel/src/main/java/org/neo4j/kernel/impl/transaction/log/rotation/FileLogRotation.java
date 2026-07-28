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
package org.neo4j.kernel.impl.transaction.log.rotation;

import java.io.IOException;
import java.time.Clock;
import java.util.function.LongSupplier;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.RotatableFile;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFile;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointLogFile;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.monitoring.Panic;

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
    private final LongSupplier lastAppendIndexSupplier;
    private final LongSupplier currentFileVersionSupplier;
    private final KernelVersionProvider kernelVersionProvider;

    public static LogRotation checkpointLogRotation(
            CheckpointLogFile checkpointLogFile,
            LogFile logFile,
            Clock clock,
            Panic databasePanic,
            LogRotationMonitor monitor,
            KernelVersionProvider kernelVersionProvider) {
        return new FileLogRotation(
                checkpointLogFile,
                LogRotationMonitor.LogType.CHECKPOINT,
                clock,
                databasePanic,
                monitor,
                logFile::getLastEntryAppendIndexInLogFiles,
                checkpointLogFile::getCurrentLogVersion,
                kernelVersionProvider);
    }

    public static LogRotation transactionLogRotation(
            LogFile logFile,
            Clock clock,
            Panic databasePanic,
            LogRotationMonitor monitor,
            KernelVersionProvider kernelVersionProvider) {
        return new FileLogRotation(
                logFile,
                LogRotationMonitor.LogType.TRANSACTIONS,
                clock,
                databasePanic,
                monitor,
                logFile::getLastEntryAppendIndexInLogFiles,
                logFile::getCurrentLogVersion,
                kernelVersionProvider);
    }

    private FileLogRotation(
            RotatableFile rotatableFile,
            LogRotationMonitor.LogType type,
            Clock clock,
            Panic databasePanic,
            LogRotationMonitor monitor,
            LongSupplier lastAppendIndexSupplier,
            LongSupplier currentFileVersionSupplier,
            KernelVersionProvider kernelVersionProvider) {
        this.clock = clock;
        this.monitor = monitor;
        this.type = type;
        this.databasePanic = databasePanic;
        this.rotatableFile = rotatableFile;
        this.lastAppendIndexSupplier = lastAppendIndexSupplier;
        this.currentFileVersionSupplier = currentFileVersionSupplier;
        this.kernelVersionProvider = kernelVersionProvider;
    }

    @Override
    public boolean rotateLogIfNeeded(LogRotateEvents logRotateEvents) throws IOException {
        /* We synchronize on the writer because we want to have a monitor that another thread
         * doing force (think batching of writes), such that it can't see a bad state of the writer
         * even when rotating underlying channels.
         */
        if (rotatableFile.rotationNeeded()) {
            synchronized (rotatableFile) {
                return locklessRotateLogIfNeeded(logRotateEvents);
            }
        }
        return false;
    }

    @Override
    public boolean locklessBatchedRotateLogIfNeeded(
            LogRotateEvents logRotateEvents,
            long lastAppendIndex,
            KernelVersion kernelVersion,
            int checksum,
            long lastTerm,
            LogFormat logFormat)
            throws IOException {
        if (rotatableFile.rotationNeeded()) {
            TransactionLogFile logFile = (TransactionLogFile) rotatableFile;
            long version = logFile.getLogRangeInfo().highestVersion();
            doRotate(
                    logRotateEvents,
                    lastAppendIndex,
                    () -> version,
                    () -> logFile.rotate(kernelVersion, lastAppendIndex, checksum, lastTerm, logFormat));
            return true;
        }
        return false;
    }

    @Override
    public boolean locklessRotateLogIfNeeded(LogRotateEvents logRotateEvents) throws IOException {
        if (rotatableFile.rotationNeeded()) {
            doRotate(
                    logRotateEvents,
                    lastAppendIndexSupplier.getAsLong(),
                    currentFileVersionSupplier,
                    rotatableFile::rotate);
            return true;
        }
        return false;
    }

    @Override
    public boolean locklessRotateLogIfNeeded(
            LogRotateEvents logRotateEvents, KernelVersion kernelVersion, boolean force) throws IOException {
        if (force || rotatableFile.rotationNeeded()) {
            doRotate(
                    logRotateEvents,
                    lastAppendIndexSupplier.getAsLong(),
                    currentFileVersionSupplier,
                    () -> rotatableFile.rotate(kernelVersion));
            return true;
        }
        return false;
    }

    @Override
    public void rotateLogFile(LogRotateEvents logRotateEvents) throws IOException {
        synchronized (rotatableFile) {
            doRotate(
                    logRotateEvents,
                    lastAppendIndexSupplier.getAsLong(),
                    currentFileVersionSupplier,
                    rotatableFile::rotate);
        }
    }

    @Override
    public void locklessRotateLogFile(
            LogRotateEvents logRotateEvents, long lastAppendIndex, int previousChecksum, long lastTerm)
            throws IOException {
        doRotate(
                logRotateEvents,
                lastAppendIndex,
                currentFileVersionSupplier,
                () -> rotatableFile.rotate(kernelVersionProvider.kernelVersion(), lastAppendIndex, previousChecksum));
    }

    @Override
    public void locklessRotateLogFile(
            LogRotateEvents logRotateEvents, KernelVersion kernelVersion, long lastAppendIndex, int previousChecksum)
            throws IOException {
        doRotate(
                logRotateEvents,
                lastAppendIndex,
                currentFileVersionSupplier,
                () -> rotatableFile.rotate(kernelVersion, lastAppendIndex, previousChecksum));
    }

    @Override
    public void locklessRotateLogFile(
            LogRotateEvents logRotateEvents,
            KernelVersion kernelVersion,
            long lastAppendIndex,
            int previousChecksum,
            long lastTerm,
            LogFormat logFormat)
            throws IOException {
        doRotate(
                logRotateEvents,
                lastAppendIndex,
                currentFileVersionSupplier,
                () -> rotatableFile.rotate(kernelVersion, lastAppendIndex, previousChecksum, lastTerm, logFormat));
    }

    @Override
    public long rotationSize() {
        return rotatableFile.rotationSize();
    }

    private void doRotate(
            LogRotateEvents logRotateEvents,
            long appendIndex,
            LongSupplier currentFileVersionSupplier,
            FileRotator fileRotator)
            throws IOException {
        try (LogRotateEvent rotateEvent = logRotateEvents.beginLogRotate()) {
            long currentVersion = currentFileVersionSupplier.getAsLong();
            /*
             * In order to rotate the log file safely we need to assert that the kernel is still
             * at full health. In case of a panic this rotation will be aborted, which is the safest alternative.
             */
            databasePanic.assertNoPanic(IOException.class);
            long startTimeMillis = clock.millis();
            monitor.startRotation(type, currentVersion);
            RotatableFile.RotationInfo logFileInfo = fileRotator.rotate();
            long millisSinceLastRotation = lastRotationCompleted == 0 ? 0 : startTimeMillis - lastRotationCompleted;
            lastRotationCompleted = clock.millis();
            long rotationElapsedTime = lastRotationCompleted - startTimeMillis;
            rotateEvent.rotationCompleted(rotationElapsedTime);
            monitor.finishLogRotation(
                    logFileInfo.file(),
                    type,
                    currentVersion,
                    logFileInfo.logHeader(),
                    appendIndex,
                    rotationElapsedTime,
                    millisSinceLastRotation);
        }
    }

    @FunctionalInterface
    private interface FileRotator {
        RotatableFile.RotationInfo rotate() throws IOException;
    }
}
