/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.log.entry;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.StoreId;

public class LogHeader {
    /**
     * The size of the version section of the header
     */
    static final int LOG_HEADER_VERSION_SIZE = Long.BYTES;

    private final byte logFormatVersion;
    private final long logVersion;
    private final long lastCommittedTxId;
    private final StoreId storeId;
    private final LogPosition startPosition;
    private final int segmentBlockSize;
    private final int previousLogFileChecksum;

    public LogHeader(
            byte logFormatVersion,
            LogPosition startPosition,
            long lastCommittedTxId,
            StoreId storeId,
            int segmentBlockSize,
            int previousLogFileChecksum) {
        this.logFormatVersion = logFormatVersion;
        this.startPosition = requireNonNull(startPosition);
        this.logVersion = startPosition.getLogVersion();
        this.lastCommittedTxId = lastCommittedTxId;
        this.storeId = storeId;
        this.segmentBlockSize = segmentBlockSize;
        this.previousLogFileChecksum = previousLogFileChecksum;
    }

    public LogHeader(LogHeader logHeader, long version) {
        this(
                logHeader.getLogFormatVersion(),
                new LogPosition(version, logHeader.getStartPosition().getByteOffset()),
                logHeader.getLastCommittedTxId(),
                logHeader.getStoreId(),
                logHeader.getSegmentBlockSize(),
                logHeader.getPreviousLogFileChecksum());
    }

    public LogPosition getStartPosition() {
        return startPosition;
    }

    public byte getLogFormatVersion() {
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
                && Objects.equals(storeId, logHeader.storeId)
                && Objects.equals(startPosition, logHeader.startPosition)
                && segmentBlockSize == logHeader.segmentBlockSize
                && previousLogFileChecksum == logHeader.previousLogFileChecksum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                logFormatVersion,
                logVersion,
                lastCommittedTxId,
                storeId,
                startPosition,
                segmentBlockSize,
                previousLogFileChecksum);
    }

    @Override
    public String toString() {
        return "LogHeader{" + "logFormatVersion="
                + logFormatVersion + ", logVersion="
                + logVersion + ", lastCommittedTxId="
                + lastCommittedTxId + ", storeId="
                + storeId + ", startPosition="
                + startPosition + ", segmentBlockSize="
                + segmentBlockSize + ", previousLogFileChecksum="
                + previousLogFileChecksum + '}';
    }
}
