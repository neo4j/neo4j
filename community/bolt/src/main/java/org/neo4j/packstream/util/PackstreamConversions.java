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

import java.util.OptionalLong;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

public final class PackstreamConversions {
    private PackstreamConversions() {}

    /**
     * Ensures that the given field is set to a list value or null.
     *
     * @param fieldName a field name as specified within the protocol documentation.
     * @param fieldValue an decoded field value.
     * @return the casted representation of the field value.
     * @throws IllegalStructArgumentException when the given field value is not a list.
     */
    public static ListValue asNullableListValue(String fieldName, AnyValue fieldValue)
            throws IllegalStructArgumentException {
        if (fieldValue == Values.NO_VALUE) {
            return null;
        }

        if (fieldValue instanceof ListValue listValue) {
            return listValue;
        }

        throw new IllegalStructArgumentException(fieldName, "Expected list");
    }

    /**
     * Ensures that the given field is set to a long value.
     *
     * @param fieldName a field name as specified within the protocol documentation.
     * @param fieldValue an decoded field value.
     * @return the casted representation of the field value.
     * @throws IllegalStructArgumentException when the given field value is null or not a long.
     */
    public static long asLong(String fieldName, Object fieldValue) throws IllegalStructArgumentException {
        PackstreamConditions.requireNonNull(fieldName, fieldValue);

        return asNullableLong(fieldName, fieldValue);
    }

    /**
     * Ensures that the given field is set to a long value or null.
     *
     * @param fieldName a field name as specified within the protocol documentation.
     * @param fieldValue an decoded field value.
     * @return the casted representation of the field value.
     * @throws IllegalStructArgumentException when the given field value is not a long.
     */
    public static Long asNullableLong(String fieldName, Object fieldValue) throws IllegalStructArgumentException {
        if (fieldValue instanceof Long longValue) {
            return longValue;
        }

        if (fieldValue == null) {
            return null;
        }

        throw new IllegalStructArgumentException(fieldName, "Expected long");
    }

    /**
     * Ensures that the given field is set to a long value.
     *
     * @param fieldName a field name as specified within the protocol documentation.
     * @param fieldValue an decoded field value.
     * @return the casted representation of the field value.
     * @throws IllegalStructArgumentException when the given field value is null or not a long.
     */
    public static long asLongValue(String fieldName, AnyValue fieldValue) throws IllegalStructArgumentException {
        if (fieldValue == Values.NO_VALUE) {
            throw new IllegalStructArgumentException(fieldName, "Expected value to be non-null");
        }

        return asNullableLongValue(fieldName, fieldValue)
                .orElseThrow(() -> new IllegalStructArgumentException(fieldName, "Expected long"));
    }

    /**
     * Ensures that the given field is set to a long value or null.
     *
     * @param fieldName a field name as specified within the protocol documentation.
     * @param fieldValue an decoded field value.
     * @return the casted representation of the field value.
     * @throws IllegalStructArgumentException when the given field value is not a long.
     */
    public static OptionalLong asNullableLongValue(String fieldName, AnyValue fieldValue)
            throws IllegalStructArgumentException {
        if (fieldValue instanceof LongValue longValue) {
            return OptionalLong.of(longValue.longValue());
        }

        if (fieldValue == Values.NO_VALUE) {
            return OptionalLong.empty();
        }

        throw new IllegalStructArgumentException(fieldName, "Expected long");
    }

    /**
     * Ensures that the given field is set to a map value or null.
     *
     * @param fieldName a field name as specified within the protocol documentation.
     * @param fieldValue an decoded field value.
     * @return the casted representation of the field value.
     * @throws IllegalStructArgumentException when the given field value is not a map.
     */
    public static MapValue asNullableMapValue(String fieldName, AnyValue fieldValue)
            throws IllegalStructArgumentException {
        if (fieldValue instanceof MapValue mapValue) {
            return mapValue;
        }

        if (fieldValue == Values.NO_VALUE) {
            return null;
        }

        throw new IllegalStructArgumentException(fieldName, "Expected dictionary");
    }

    /**
     * Ensures that the given field is set to a string value.
     *
     * @param fieldName a field name as specified within the protocol documentation.
     * @param fieldValue an decoded field value.
     * @return the casted representation of the field value.
     * @throws IllegalStructArgumentException when the given field value is null or not a string.
     */
    public static String asString(String fieldName, Object fieldValue) throws IllegalStructArgumentException {
        PackstreamConditions.requireNonNull(fieldName, fieldValue);

        return asNullableString(fieldName, fieldValue);
    }

    /**
     * Ensures that the given field is set to a string value or null.
     *
     * @param fieldName a field name as specified within the protocol documentation.
     * @param fieldValue an decoded field value.
     * @return the casted representation of the field value.
     * @throws IllegalStructArgumentException when the given field value is not a string.
     */
    public static String asNullableString(String fieldName, Object fieldValue) throws IllegalStructArgumentException {
        if (fieldValue instanceof String stringValue) {
            return stringValue;
        }

        if (fieldValue == null) {
            return null;
        }

        throw new IllegalStructArgumentException(fieldName, "Expected string");
    }

    /**
     * Ensures that the given field is set to a string value or null.
     *
     * @param fieldName a field name as specified within the protocol documentation.
     * @param fieldValue an decoded field value.
     * @return the casted representation of the field value.
     * @throws IllegalStructArgumentException when the given field value is not a string.
     */
    public static String asNullableStringValue(String fieldName, AnyValue fieldValue)
            throws IllegalStructArgumentException {
        if (fieldValue instanceof TextValue textValue) {
            return textValue.stringValue();
        }

        if (fieldValue == Values.NO_VALUE) {
            return null;
        }

        throw new IllegalStructArgumentException(fieldName, "Expected string");
    }
}
