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
package org.neo4j.bolt.protocol.io;

/**
 * Provides a listing of recognized
 */
public enum BoltStructType {
    NODE('N', 4, 3),
    RELATIONSHIP('R', 8, 5),
    UNBOUND_RELATIONSHIP('r', 4, 3),
    PATH('P', 3);

    private final short tag;
    private final short defaultSize;

    @Deprecated(since = "5.0", forRemoval = true)
    private final short legacySize;

    BoltStructType(char tag, int defaultSize, @Deprecated(since = "5.0", forRemoval = true) int legacySize) {
        this.tag = (short) tag;
        this.defaultSize = (short) defaultSize;
        this.legacySize = (short) legacySize;
    }

    BoltStructType(char tag, int defaultSize) {
        this(tag, defaultSize, defaultSize);
    }

    public short getTag() {
        return tag;
    }

    public short getDefaultSize() {
        return defaultSize;
    }

    @Deprecated(since = "5.0", forRemoval = true)
    public short getLegacySize() {
        return legacySize;
    }
}
