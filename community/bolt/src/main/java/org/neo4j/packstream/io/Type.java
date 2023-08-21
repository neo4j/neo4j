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
package org.neo4j.packstream.io;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public enum Type {
    /**
     * Fallback value which is used when a reserved type marker is encountered.
     */
    RESERVED,

    /**
     * Fallback value which is used when a null value type marker is encountered.
     */
    NONE,

    BYTES,
    BOOLEAN,
    FLOAT,
    INT,
    LIST,
    MAP,
    STRING,
    STRUCT;

    public static final long TINY_INT_MIN = -16;
    public static final long TINY_INT_MAX = 127;
    public static final long INT8_MIN = Byte.MIN_VALUE;
    public static final long INT8_MAX = Byte.MAX_VALUE;
    public static final long INT16_MIN = Short.MIN_VALUE;
    public static final long INT16_MAX = Short.MAX_VALUE;
    public static final long INT32_MIN = Integer.MIN_VALUE;
    public static final long INT32_MAX = Integer.MAX_VALUE;
    public static final long INT64_MIN = Long.MIN_VALUE;
    public static final long INT64_MAX = Long.MAX_VALUE;

    public static final Charset STRING_CHARSET = StandardCharsets.UTF_8;
}
