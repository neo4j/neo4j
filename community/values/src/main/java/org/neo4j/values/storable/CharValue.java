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
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.values.utils.ValueMath.HASH_CONSTANT;
import static org.neo4j.values.virtual.VirtualValues.list;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.neo4j.hashing.HashFunction;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.virtual.ListValue;

public final class CharValue extends TextValue {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(CharValue.class);

    private final char value;

    CharValue(char value) {
        this.value = value;
    }

    @Override
    public boolean equalTo(Object other) {
        return other instanceof Value && equals((Value) other);
    }

    @Override
    public boolean equals(Value other) {
        return other.equals(value);
    }

    @Override
    public boolean equals(char x) {
        return value == x;
    }

    @Override
    public boolean equals(String x) {
        return x.length() == 1 && x.charAt(0) == value;
    }

    @Override
    protected int computeHashToMemoize() {
        // The 31 is there to give it the same hash as the string equivalent
        return HASH_CONSTANT + value;
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        return updateHash(hashFunction, hash, value);
    }

    public static long updateHash(HashFunction hashFunction, long hash, char value) {
        hash = hashFunction.update(hash, value);
        return hashFunction.update(hash, 1); // Pretend we're a string of length 1.
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writer.writeString(value);
    }

    @Override
    public Object asObjectCopy() {
        return value;
    }

    @Override
    public String prettyPrint() {
        return format("'%s'", value);
    }

    @Override
    public String stringValue() {
        return Character.toString(value);
    }

    @Override
    public int length() {
        return 1;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public TextValue substring(int start, int length) {
        if (length != 1 && start != 0) {
            return StringValue.EMPTY;
        }

        return this;
    }

    @Override
    public TextValue trim() {
        if (Character.isWhitespace(value)) {
            return StringValue.EMPTY;
        } else {
            return this;
        }
    }

    @Override
    public TextValue ltrim() {
        return trim();
    }

    @Override
    public TextValue rtrim() {
        return trim();
    }

    @Override
    public TextValue toLower() {
        return new CharValue(Character.toLowerCase(value));
    }

    @Override
    public TextValue toUpper() {
        return new CharValue(Character.toUpperCase(value));
    }

    @Override
    public ListValue split(String separator) {
        if (separator.equals(stringValue())) {
            return EMPTY_SPLIT;
        } else {
            return list(this);
        }
    }

    @Override
    public ListValue split(List<String> separators) {
        if (separators.stream().anyMatch(sep -> sep.equals(stringValue()))) {
            return EMPTY_SPLIT;
        } else {
            return list(this);
        }
    }

    @Override
    public TextValue replace(String find, String replace) {
        assert find != null;
        assert replace != null;
        if (stringValue().equals(find)) {
            return Values.stringValue(replace);
        } else {
            return this;
        }
    }

    @Override
    public TextValue reverse() {
        return this;
    }

    @Override
    public TextValue plus(TextValue other) {
        return Values.stringValue(value + other.stringValue());
    }

    @Override
    public boolean startsWith(TextValue other) {
        return other.length() == 1 && other.stringValue().charAt(0) == value;
    }

    @Override
    public boolean endsWith(TextValue other) {
        return startsWith(other);
    }

    @Override
    public boolean contains(TextValue other) {
        return startsWith(other);
    }

    public char value() {
        return value;
    }

    @Override
    public int compareTo(TextValue other) {
        return TextValues.compareCharToString(value, other.stringValue());
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapChar(this);
    }

    @Override
    protected Matcher matcher(Pattern pattern) {
        return pattern.matcher(String.valueOf(value));
    }

    @Override
    public String toString() {
        return format("%s('%s')", getTypeName(), value);
    }

    @Override
    public String getTypeName() {
        return "Char";
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE;
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return ValueRepresentation.UTF16_TEXT;
    }
}
