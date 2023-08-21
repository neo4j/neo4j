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
package org.neo4j.index.internal.gbptree;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Main point of interaction for customizing a {@link GBPTree}, how its keys and values are represented
 * as bytes and what keys and values contains.
 * <p>
 * Additionally custom meta data can be supplied, which will be persisted in {@link GBPTree}.
 * <p>
 * Rather extend {@link Adapter} as to get standard implementation of e.g. {@link Adapter#toString()}.
 *
 * @param <KEY> type of key
 * @param <VALUE> type of value
 */
public interface Layout<KEY, VALUE> extends KeyLayout<KEY> {
    int FIXED_SIZE_VALUE = -1;

    /**
     * @return new value instance.
     */
    VALUE newValue();

    /**
     * @param value for which to give size.
     * @return size, in bytes, of given value.
     */
    int valueSize(VALUE value);

    /**
     * Writes contents of {@code value} into {@code cursor} at its current offset.
     *
     * @param cursor {@link PageCursor} to write into, at current offset.
     * @param value value containing data to write.
     */
    void writeValue(PageCursor cursor, VALUE value);

    /**
     * Reads value contents at {@code cursor} at its current offset into {@code value}.
     * @param cursor {@link PageCursor} to read from, at current offset.
     * @param into value instances to read into.
     * @param valueSize size of key to read or {@link #FIXED_SIZE_VALUE} if value is fixed size.
     */
    void readValue(PageCursor cursor, VALUE into, int valueSize);

    /**
     * Utility method for generating an {@link #identifier()}. Generates an 8-byte identifier from a short name
     * plus a 4-byte identifier.
     *
     * @param name name to be part of this identifier, must at most be 4 characters.
     * @param identifier to include into the returned named identifier.
     * @return a long which is a combination of {@code name} and {@code identifier}.
     */
    static long namedIdentifier(String name, int identifier) {
        char[] chars = name.toCharArray();
        if (chars.length > 4) {
            throw new IllegalArgumentException("Maximum 4 character name, was '" + name + "'");
        }
        long upperInt = 0;
        for (char aChar : chars) {
            byte byteValue = (byte) (((byte) aChar) ^ ((byte) (aChar >> 8)));
            upperInt <<= 8;
            upperInt |= byteValue & 0xFF;
        }

        return (upperInt << Integer.SIZE) | identifier;
    }

    /**
     * Adapter for {@link Layout}, which contains convenient standard implementations of some methods.
     *
     * @param <KEY> type of key
     * @param <VALUE> type of value
     */
    abstract class Adapter<KEY, VALUE> extends KeyLayout.Adapter<KEY> implements Layout<KEY, VALUE> {
        protected Adapter(boolean fixedSize, long identifier, int majorVersion, int minorVersion) {
            super(fixedSize, identifier, majorVersion, minorVersion);
        }
    }
}
