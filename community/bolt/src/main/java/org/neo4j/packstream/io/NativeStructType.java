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
package org.neo4j.packstream.io;

import static java.util.function.Function.identity;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum NativeStructType {
    DATE('D', 1),
    TIME('T', 2),
    LOCAL_TIME('t', 1),
    DATE_TIME('F', 3),
    DATE_TIME_ZONE_ID('f', 3),
    LOCAL_DATE_TIME('d', 2),
    DURATION('E', 4),
    POINT_2D('X', 3),
    POINT_3D('Y', 4);

    private static final Map<Short, NativeStructType> tagMap;
    private final short tag;
    private final short defaultSize;

    static {
        tagMap = Stream.of(values()).collect(Collectors.toMap(NativeStructType::getTag, identity()));
    }

    NativeStructType(char tag, int defaultSize) {
        this.tag = (short) tag;
        this.defaultSize = (short) defaultSize;
    }

    public short getTag() {
        return this.tag;
    }

    public short getDefaultSize() {
        return defaultSize;
    }

    /**
     * Retrieves a given native struct type based on its assigned tag number.
     *
     * @param tag a tag.
     * @return a native struct type or, if no such native struct exists, null.
     */
    public static NativeStructType byTag(short tag) {
        return tagMap.get(tag);
    }
}
