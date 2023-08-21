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
package org.neo4j.values.storable;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.hashing.HashFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.Comparison;
import org.neo4j.values.Equality;
import org.neo4j.values.SequenceValue;

public abstract class Value extends AnyValue {
    private static final Pattern MAP_PATTERN = Pattern.compile("\\{(.*)}");

    private static final Pattern KEY_VALUE_PATTERN =
            Pattern.compile("(?:\\A|,)\\s*+(?<k>[a-z_A-Z]\\w*+)\\s*:\\s*(?<v>[^\\s,]+)");

    static final Pattern QUOTES_PATTERN = Pattern.compile("^[\"']|[\"']$");

    @Override
    public boolean equalTo(Object other) {
        return other instanceof Value && equals((Value) other);
    }

    public abstract boolean equals(Value other);

    public boolean equals(byte[] x) {
        return false;
    }

    public boolean equals(short[] x) {
        return false;
    }

    public boolean equals(int[] x) {
        return false;
    }

    public boolean equals(long[] x) {
        return false;
    }

    public boolean equals(float[] x) {
        return false;
    }

    public boolean equals(double[] x) {
        return false;
    }

    public boolean equals(boolean x) {
        return false;
    }

    public boolean equals(boolean[] x) {
        return false;
    }

    public boolean equals(long x) {
        return false;
    }

    public boolean equals(double x) {
        return false;
    }

    public boolean equals(char x) {
        return false;
    }

    public boolean equals(String x) {
        return false;
    }

    public boolean equals(char[] x) {
        return false;
    }

    public boolean equals(String[] x) {
        return false;
    }

    public boolean equals(Geometry[] x) {
        return false;
    }

    public boolean equals(ZonedDateTime[] x) {
        return false;
    }

    public boolean equals(LocalDate[] x) {
        return false;
    }

    public boolean equals(DurationValue[] x) {
        return false;
    }

    public boolean equals(LocalDateTime[] x) {
        return false;
    }

    public boolean equals(LocalTime[] x) {
        return false;
    }

    public boolean equals(OffsetTime[] x) {
        return false;
    }

    @Override
    public Equality ternaryEquals(AnyValue other) {
        assert other != null : "null values are not supported, use NoValue.NO_VALUE instead";
        if (other == NO_VALUE) {
            return Equality.UNDEFINED;
        }
        if (other.isSequenceValue() && this.isSequenceValue()) {
            return ((SequenceValue) this).ternaryEquality((SequenceValue) other);
        }
        if (hasNaNOperand(this, other)) {
            return Equality.FALSE;
        }
        if (other instanceof Value otherValue && otherValue.valueGroup() == valueGroup()) {
            if (this.ternaryUndefined() || otherValue.ternaryUndefined()) {
                return Equality.UNDEFINED;
            }
            return equals(otherValue) ? Equality.TRUE : Equality.FALSE;
        }
        return Equality.FALSE;
    }

    protected abstract int unsafeCompareTo(Value other);

    /**
     * Should return {@code Comparison.UNDEFINED} for values that cannot be compared
     * under Comparability semantics.
     */
    Comparison unsafeTernaryCompareTo(Value other) {
        if (ternaryUndefined() || other.ternaryUndefined()) {
            return Comparison.UNDEFINED;
        }
        return Comparison.from(unsafeCompareTo(other));
    }

    boolean ternaryUndefined() {
        return false;
    }

    @Override
    public <E extends Exception> void writeTo(AnyValueWriter<E> writer) throws E {
        writeTo((ValueWriter<E>) writer);
    }

    public abstract <E extends Exception> void writeTo(ValueWriter<E> writer) throws E;

    /**
     * Return this value as a regular java boxed primitive, String or primitive array. This method performs defensive
     * copying when needed, so the returned value is safe to modify.
     *
     * @return the object version of the current value
     */
    public abstract Object asObjectCopy();

    /**
     * Return this value as a regular java boxed primitive, String or primitive array. This method does not clone
     * primitive arrays.
     *
     * @return the object version of the current value
     */
    public Object asObject() {
        return asObjectCopy();
    }

    /**
     * Returns a json-like string representation of the current value.
     */
    public abstract String prettyPrint();

    @Override
    public abstract ValueRepresentation valueRepresentation();

    public final ValueGroup valueGroup() {
        return valueRepresentation().valueGroup();
    }

    public abstract NumberType numberType();

    /**
     * Returns whether or not the type of this value is the same as the type of the given value. Value type is more specific than
     * what {@link #valueGroup()} returns, but less granular than, say specific class. For example there are specific classes for
     * representing string values for various scenarios, but they're all strings... same type.
     *
     * @param value {@link Value} to compare type against.
     * @return {@code true} if the given {@code value} is of the same value type as this value.
     */
    public boolean isSameValueTypeAs(Value value) {
        return getClass() == value.getClass();
    }

    public final long hashCode64() {
        HashFunction xxh64 = HashFunction.incrementalXXH64();
        long seed = 1; // Arbitrary seed, but it must always be the same or hash values will change.
        return xxh64.finalise(updateHash(xxh64, xxh64.initialise(seed)));
    }

    public abstract long updateHash(HashFunction hashFunction, long hash);

    /**
     * Parses a json-like string representing a map, into {@link Map} with {@link String} keys and values.
     * Text should start and end with curly brackets. Text can contain multiple key/value pairs, separated by a comma.
     *
     * @param text textual representation of a map to parse into a {@link Map}.
     * @return the parsed text as a {@link Map}.
     */
    public static Map<String, String> parseStringMap(CharSequence text) {
        Matcher mapMatcher = MAP_PATTERN.matcher(text);
        String errorMessage = format("Failed to parse map value: '%s'", text);
        if (!(mapMatcher.find() && mapMatcher.groupCount() == 1)) {
            throw new InvalidArgumentException(errorMessage);
        }

        String mapContents = mapMatcher.group(1);
        if (mapContents.isEmpty()) {
            throw new InvalidArgumentException(errorMessage);
        }

        Map<String, String> data = new HashMap<>();
        var length = mapContents.length();
        int i = 0;
        while (i < length) {
            // Parse the key
            var end = mapContents.indexOf(':', i);
            if (end == -1) {
                break;
            }
            var key = mapContents.substring(i, end).trim();
            i = end + 1;

            checkParseState(text, i, i < length);
            while (i < length && Character.isWhitespace(mapContents.charAt(i))) {
                i++;
            }
            var firstChar = mapContents.charAt(i);
            if (firstChar == '\'' || firstChar == '"') {
                i++;
                end = mapContents.indexOf(firstChar, i);
            } else {
                end = mapContents.indexOf(',', i);
                end = end == -1 ? mapContents.length() : end;
                checkParseState(text, i, i != end);
                while (Character.isWhitespace(mapContents.charAt(end - 1))) {
                    end--;
                }
            }
            checkParseState(text, i, end != -1);
            var value = mapContents.substring(i, end);

            if (data.containsKey(key)) {
                throw new InvalidArgumentException(format("Duplicate field '%s'", key));
            }
            data.put(key, value);
            i = end + 1;
            while (i < length && (Character.isWhitespace(mapContents.charAt(i)) || mapContents.charAt(i) == ',')) {
                i++;
            }
        }
        return data;
    }

    private static void checkParseState(CharSequence text, int i, boolean condition) {
        if (!condition) {
            throw new InvalidArgumentException(format(
                    "Was expecting key:value, key:'value' or key:\"value\" pairs in %s. Error near index %d", text, i));
        }
    }
}
