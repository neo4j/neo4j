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
package org.neo4j.kernel.impl.transaction.log.entry.v57;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.CHUNK_START;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.AbstractVersionAwareLogEntry;
import org.neo4j.string.Mask;

public class LogEntryChunkStart extends AbstractVersionAwareLogEntry {
    private final long timeWritten;
    private final long chunkId;
    private final LogPosition previousBatchLogPosition;

    public LogEntryChunkStart(
            KernelVersion kernelVersion, long timeWritten, long chunkId, LogPosition previousBatchLogPosition) {
        super(kernelVersion, CHUNK_START);
        this.timeWritten = timeWritten;
        this.chunkId = chunkId;
        this.previousBatchLogPosition = previousBatchLogPosition;
    }

    public long getTimeWritten() {
        return timeWritten;
    }

    public long getChunkId() {
        return chunkId;
    }

    public LogPosition getPreviousBatchLogPosition() {
        return previousBatchLogPosition;
    }

    @Override
    public String toString(Mask mask) {
        return "LogEntryChunkStart{" + "timeWritten="
                + timeWritten + ", chunkId="
                + chunkId + ", previousBatchLogPosition="
                + previousBatchLogPosition + '}';
    }
}
