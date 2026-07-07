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
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.neo4j.kernel.impl.transaction.log.entry.LogSegments.UNKNOWN_LOG_SEGMENT_SIZE;

import java.util.Objects;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.StoreIdentifier;

public class LogHeader {
    /**
     * The size of the version section of the header
     */
    static final int LOG_HEADER_VERSION_SIZE = Long.BYTES;

    public static final long UNKNOWN_TERM = -1L;
    public static final long UNSPECIFIED_CREATION_TIME = 0L;

    private final LogFormat logFormatVersion;
    private final long logVersion;
    private final long lastAppendIndex;
    private final StoreIdentifier storeIdentifier;
    private final LogPosition startPosition;
    private final int segmentBlockSize;
    private final long lastTerm;
    private final int previousLogFileChecksum;
    private final KernelVersion kernelVersion;
    private final long creationTime;

    LogHeader(
            byte logFormatVersion,
            long logVersion,
            long lastAppendIndex,
            long lastTerm,
            StoreIdentifier storeIdentifier,
            long headerSize,
            int segmentBlockSize,
            int previousLogFileChecksum,
            KernelVersion kernelVersion,
            long creationTime) {
        this.logFormatVersion = LogFormat.fromByteVersion(logFormatVersion);
        this.logVersion = logVersion;
        this.lastAppendIndex = lastAppendIndex;
        this.storeIdentifier = storeIdentifier;
        this.segmentBlockSize = segmentBlockSize;
        this.lastTerm = lastTerm;
        if (segmentBlockSize != UNKNOWN_LOG_SEGMENT_SIZE) {
            // If we have a segmented file we should start reading after the first segment
            this.startPosition = new LogPosition(logVersion, segmentBlockSize);
        } else {
            this.startPosition = new LogPosition(logVersion, headerSize);
        }
        this.previousLogFileChecksum = previousLogFileChecksum;
        this.kernelVersion = kernelVersion;
        this.creationTime = creationTime;
    }

    public LogHeader(LogHeader logHeader, long version) {
        logFormatVersion = logHeader.logFormatVersion;
        logVersion = version;
        lastAppendIndex = logHeader.lastAppendIndex;
        storeIdentifier = logHeader.storeIdentifier;
        segmentBlockSize = logHeader.segmentBlockSize;
        startPosition = new LogPosition(version, logHeader.startPosition.getByteOffset());
        previousLogFileChecksum = logHeader.previousLogFileChecksum;
        kernelVersion = logHeader.kernelVersion;
        lastTerm = logHeader.lastTerm;
        creationTime = logHeader.creationTime;
    }

    public LogHeader(LogHeader logHeader, long version, long newLastAppendIndex, int checksum) {
        logFormatVersion = logHeader.logFormatVersion;
        logVersion = version;
        lastAppendIndex = newLastAppendIndex;
        storeIdentifier = logHeader.storeIdentifier;
        segmentBlockSize = logHeader.segmentBlockSize;
        startPosition = new LogPosition(version, logHeader.startPosition.getByteOffset());
        previousLogFileChecksum = checksum;
        kernelVersion = logHeader.kernelVersion;
        lastTerm = logHeader.lastTerm;
        creationTime = logHeader.creationTime;
    }

    public LogPosition getStartPosition() {
        return startPosition;
    }

    public LogFormat getLogFormatVersion() {
        return logFormatVersion;
    }

    public long getLogVersion() {
        return logVersion;
    }

    public StoreIdentifier getStoreIdentifier() {
        return storeIdentifier;
    }

    public int getSegmentBlockSize() {
        return segmentBlockSize;
    }

    public int getPreviousLogFileChecksum() {
        return previousLogFileChecksum;
    }

    public KernelVersion getKernelVersion() {
        return kernelVersion;
    }

    public long getLastAppendIndex() {
        return lastAppendIndex;
    }

    public long getLastTerm() {
        return lastTerm;
    }

    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogHeader logHeader = (LogHeader) o;
        return logFormatVersion == logHeader.logFormatVersion
                && logVersion == logHeader.logVersion
                && lastAppendIndex == logHeader.lastAppendIndex
                && Objects.equals(storeIdentifier, logHeader.storeIdentifier)
                && Objects.equals(startPosition, logHeader.startPosition)
                && segmentBlockSize == logHeader.segmentBlockSize
                && previousLogFileChecksum == logHeader.previousLogFileChecksum
                && kernelVersion == logHeader.kernelVersion
                && lastTerm == logHeader.lastTerm;
        // The creationTime is not involved in equality checking for fears around flaky behaviour
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                logFormatVersion,
                logVersion,
                lastAppendIndex,
                storeIdentifier,
                startPosition,
                segmentBlockSize,
                previousLogFileChecksum,
                kernelVersion,
                lastTerm);
        // The creationTime is not involved in equality checking for fears around flaky behaviour
    }

    @Override
    public String toString() {
        return "LogHeader{" + "logFormatVersion=" + logFormatVersion + ", logVersion=" + logVersion
                + ", lastAppendIndex=" + lastAppendIndex + ", lastTerm="
                + lastTerm + ", storeIdentifier=" + storeIdentifier + ", startPosition=" + startPosition
                + ", segmentBlockSize="
                + segmentBlockSize + ", previousLogFileChecksum=" + previousLogFileChecksum + ", kernelVersion="
                + kernelVersion + ", creationTime=" + creationTime + '}';
    }
}
