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
import org.neo4j.storageengine.api.StoreId;

public class LogHeader {
    /**
     * The size of the version section of the header
     */
    static final int LOG_HEADER_VERSION_SIZE = Long.BYTES;

    private final LogFormat logFormatVersion;
    private final long logVersion;
    private final long lastCommittedTxId;
    private final long lastAppendIndex;
    private final StoreId storeId;
    private final LogPosition startPosition;
    private final int segmentBlockSize;
    private final int previousLogFileChecksum;
    private final KernelVersion kernelVersion;

    LogHeader(
            byte logFormatVersion,
            long logVersion,
            long lastCommittedTxId,
            long lastAppendIndex,
            StoreId storeId,
            long headerSize,
            int segmentBlockSize,
            int previousLogFileChecksum,
            KernelVersion kernelVersion) {
        this.logFormatVersion = LogFormat.fromByteVersion(logFormatVersion);
        this.logVersion = logVersion;
        this.lastCommittedTxId = lastCommittedTxId;
        this.lastAppendIndex = lastAppendIndex;
        this.storeId = storeId;
        this.segmentBlockSize = segmentBlockSize;
        if (segmentBlockSize != UNKNOWN_LOG_SEGMENT_SIZE) {
            // If we have a segmented file we should start reading after the first segment
            this.startPosition = new LogPosition(logVersion, segmentBlockSize);
        } else {
            this.startPosition = new LogPosition(logVersion, headerSize);
        }
        this.previousLogFileChecksum = previousLogFileChecksum;
        this.kernelVersion = kernelVersion;
    }

    public LogHeader(LogHeader logHeader, long version) {
        logFormatVersion = logHeader.logFormatVersion;
        logVersion = version;
        lastCommittedTxId = logHeader.lastCommittedTxId;
        lastAppendIndex = logHeader.lastAppendIndex;
        storeId = logHeader.storeId;
        segmentBlockSize = logHeader.segmentBlockSize;
        startPosition = new LogPosition(version, logHeader.startPosition.getByteOffset());
        previousLogFileChecksum = logHeader.previousLogFileChecksum;
        kernelVersion = logHeader.kernelVersion;
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

    public long getLastCommittedTxId() {
        return lastCommittedTxId;
    }

    public StoreId getStoreId() {
        return storeId;
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
                && lastCommittedTxId == logHeader.lastCommittedTxId
                && lastAppendIndex == logHeader.lastAppendIndex
                && Objects.equals(storeId, logHeader.storeId)
                && Objects.equals(startPosition, logHeader.startPosition)
                && segmentBlockSize == logHeader.segmentBlockSize
                && previousLogFileChecksum == logHeader.previousLogFileChecksum
                && kernelVersion == logHeader.kernelVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                logFormatVersion,
                logVersion,
                lastCommittedTxId,
                lastAppendIndex,
                storeId,
                startPosition,
                segmentBlockSize,
                previousLogFileChecksum,
                kernelVersion);
    }

    @Override
    public String toString() {
        return "LogHeader{" + "logFormatVersion=" + logFormatVersion + ", logVersion=" + logVersion
                + ", lastCommittedTxId=" + lastCommittedTxId + ", lastAppendIndex="
                + lastAppendIndex + ", storeId=" + storeId + ", startPosition=" + startPosition + ", segmentBlockSize="
                + segmentBlockSize
                + ", previousLogFileChecksum=" + previousLogFileChecksum + ", kernelVersion=" + kernelVersion + '}';
    }
}
