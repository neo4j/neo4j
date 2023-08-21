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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;

public interface ReadableLogPositionAwareChannel extends ReadableChannel, LogPositionAwareChannel {
    /**
     * Logically, this method is the same as calling
     * {@link LogPositionAwareChannel#getCurrentLogPosition(LogPositionMarker)} followed by a call to
     * {@link ReadableChannel#getVersion()}. However, in some circumstances the call to get can cause the channel to
     * rollover into the next version when the marker has been positioned in the PREVIOUS channel, giving an
     * inconsistent reading. Implementations should ensure that the positioned marker is correct for the location of
     * the returned byte value.
     * @param marker the marker used to track the position of the underlying channel BEFORE getting the byte value
     * @return the next byte value in the channel
     * @throws IOException if unable to read the channel for data
     */
    default byte markAndGetVersion(LogPositionMarker marker) throws IOException {
        getCurrentLogPosition(marker);
        return getVersion();
    }
}
