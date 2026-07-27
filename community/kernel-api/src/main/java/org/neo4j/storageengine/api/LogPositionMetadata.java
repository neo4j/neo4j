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
package org.neo4j.storageengine.api;

import static org.neo4j.kernel.impl.transaction.log.LogPosition.UNSPECIFIED;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

/**
 * Metadata concerning the position of an entry in a log. If log position is not available on creation, then use of the
 * {@link #metadataWithJustAppendIndex(long)} method is encouraged as an alternative to manually constructing a new
 * instance of this class.
 *
 * @param appendIndex   the append index of the log entry.
 * @param prePosition   the {@link LogPosition} of the start of the entry.
 * @param postPosition  the {@link LogPosition} of the end of the entry.
 * @param checksum      the checksum of the log entry.
 * @param kernelVersion the version of the log entry.
 */
public record LogPositionMetadata(
        long appendIndex,
        LogPosition prePosition,
        LogPosition postPosition,
        int checksum,
        KernelVersion kernelVersion) {
    private static final int UNKNOWN_CHECKSUM = -1;

    public static final LogPositionMetadata NO_METADATA =
            new LogPositionMetadata(UNKNOWN_APPEND_INDEX, UNSPECIFIED, UNSPECIFIED, UNKNOWN_CHECKSUM, null);

    /**
     * Returns a new {@link LogPositionMetadata} with a usable append index, but invalid positional/checksum data. If
     * log position is available on creation, then use of the {@link #LogPositionMetadata(long, LogPosition, LogPosition, int, KernelVersion)} constructor is encouraged.
     *
     * @param appendIndex the append index of the log entry.
     */
    public static LogPositionMetadata metadataWithJustAppendIndex(long appendIndex) {
        if (appendIndex == UNKNOWN_APPEND_INDEX) {
            // We already have an object for that - no need to allocate more
            return NO_METADATA;
        }

        return new LogPositionMetadata(
                appendIndex, LogPosition.UNSPECIFIED, LogPosition.UNSPECIFIED, UNKNOWN_CHECKSUM, null);
    }

    /**
     * @return {@code true} if both {@link #prePosition} and {@link #postPosition} have been set to valid values.
     */
    public boolean hasValidPositionData() {
        return prePosition != null && prePosition != UNSPECIFIED && postPosition != null && postPosition != UNSPECIFIED;
    }
}
