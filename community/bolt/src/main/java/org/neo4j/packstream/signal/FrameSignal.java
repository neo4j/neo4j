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
package org.neo4j.packstream.signal;

/**
 * Provides a marker for signals which are encoded as part of the chunk encoding layer.
 */
public enum FrameSignal {
    /**
     * Marks the end of a message causing an empty termination chunk to be flushed down the wire.
     */
    MESSAGE_END(0x0000),

    /**
     * Marks a no-op chunk which may be transmitted between messages if a connection goes unused for extended periods of time.
     */
    NOOP(0x0000, true);

    private final int tag;
    private final boolean requiresCleanState;

    FrameSignal(int tag, boolean requiresCleanState) {
        this.tag = tag;
        this.requiresCleanState = requiresCleanState;
    }

    FrameSignal(int tag) {
        this(tag, false);
    }

    public int getTag() {
        return this.tag;
    }

    public boolean requiresCleanState() {
        return this.requiresCleanState;
    }
}
