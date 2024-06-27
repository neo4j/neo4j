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
package org.neo4j.kernel.impl.transaction.log.checkpoint.volume;

import static org.neo4j.io.ByteUnit.bytesToString;

import java.util.concurrent.TimeUnit;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.checkpoint.AbstractCheckPointThreshold;

public class VolumeCheckPointThreshold extends AbstractCheckPointThreshold {
    private final long volumeBytes;
    private final long fileSizeBytes;
    private volatile LogPosition checkpointLogPosition;

    public VolumeCheckPointThreshold(long volumeBytes, long fileSizeBytes) {
        super("every " + bytesToString(volumeBytes) + " of transaction logs.");
        this.volumeBytes = volumeBytes;
        this.fileSizeBytes = fileSizeBytes;
    }

    @Override
    protected boolean thresholdReached(long lastAppendIndex, LogPosition logPosition) {
        var previousLogPosition = checkpointLogPosition;
        long files = Math.abs(logPosition.getLogVersion() - previousLogPosition.getLogVersion());
        long offset = logPosition.getByteOffset() - previousLogPosition.getByteOffset();
        long bytesDiff = Math.abs(files * fileSizeBytes + offset);
        return volumeBytes < bytesDiff;
    }

    @Override
    public void initialize(long appendIndex, LogPosition logPosition) {
        checkpointLogPosition = logPosition;
    }

    @Override
    public void checkPointHappened(long appendIndex, LogPosition logPosition) {
        checkpointLogPosition = logPosition;
    }

    @Override
    public long checkFrequencyMillis() {
        return TimeUnit.SECONDS.toMillis(1);
    }
}
