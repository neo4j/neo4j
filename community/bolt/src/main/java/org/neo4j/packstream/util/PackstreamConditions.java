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
package org.neo4j.packstream.util;

import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

public final class PackstreamConditions {
    private PackstreamConditions() {}

    /**
     * Ensures that a given struct header declares the expected number of fields.
     *
     * @param header a decoded structure header.
     * @param expected an expected number of fields.
     * @throws IllegalStructSizeException when the header does not declare exactly the expected number of fields.
     */
    public static void requireLength(StructHeader header, int expected) throws IllegalStructSizeException {
        if (header.length() != expected) {
            throw new IllegalStructSizeException(expected, header.length());
        }
    }

    /**
     * Ensures that a given struct header declares no fields.
     *
     * @param header a decoded structure header.
     * @throws IllegalStructSizeException when the header declares one or more fields.
     */
    public static void requireEmpty(StructHeader header) throws IllegalStructSizeException {
        requireLength(header, 0);
    }

    /**
     * Ensures that a given struct field is not null.
     *
     * @param fieldName a field name as specified within the protocol documentation.
     * @param fieldValue a decoded field value.
     * @throws IllegalStructArgumentException when the field value is null.
     */
    public static void requireNonNull(String fieldName, Object fieldValue) throws IllegalStructArgumentException {
        if (fieldValue == null) {
            throw new IllegalStructArgumentException(fieldName, "Expected value to be non-null");
        }
    }

    /**
     * Ensures that a given struct field is not null.
     *
     * @param fieldName a field name as specified within the protocol documentation.
     * @param fieldValue a decoded field value.
     * @throws IllegalStructArgumentException when the field value is null.
     */
    public static void requireNonNullValue(String fieldName, AnyValue fieldValue)
            throws IllegalStructArgumentException {
        if (fieldValue == Values.NO_VALUE) {
            throw new IllegalStructArgumentException(fieldName, "Expected value to be non-null");
        }
    }
}
