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
import static org.neo4j.memory.HeapEstimator.sizeOf;

import java.util.Arrays;
import org.neo4j.hashing.HashFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;

public final class CharArray extends TextArray {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(CharArray.class);

    private final char[] value;

    CharArray(char[] value) {
        assert value != null;
        this.value = value;
    }

    @Override
    public boolean equals(SequenceValue other) {
        if (other instanceof ArrayValue otherArray) {
            return otherArray.equals(value);
        } else {
            return super.equals(other);
        }
    }

    @Override
    public boolean equals(Value other) {
        return other.equals(value);
    }

    @Override
    public boolean equals(char[] x) {
        return Arrays.equals(value, x);
    }

    @Override
    public boolean equals(String[] x) {
        return PrimitiveArrayValues.equals(value, x);
    }

    @Override
    protected int computeHashToMemoize() {
        return NumberValues.hash(value);
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        hash = hashFunction.update(hash, value.length);
        for (char c : value) {
            hash = CharValue.updateHash(hashFunction, hash, c);
        }
        return hash;
    }

    @Override
    public int intSize() {
        return value.length;
    }

    @Override
    public String stringValue(int offset) {
        return Character.toString(value[offset]);
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        PrimitiveArrayWriting.writeTo(writer, value);
    }

    @Override
    public char[] asObjectCopy() {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    @Deprecated
    public char[] asObject() {
        return value;
    }

    @Override
    public String prettyPrint() {
        if (isEmpty()) {
            return "[]";
        }

        final var sb = new StringBuilder(this.intSize());
        sb.append('[').append(value(0).prettyPrint());
        for (int i = 1; i < this.intSize(); i++) {
            sb.append(", ").append(value(i).prettyPrint());
        }
        return sb.append(']').toString();
    }

    @Override
    public CharValue value(int position) {
        return Values.charValue(value[position]);
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapCharArray(this);
    }

    @Override
    public String toString() {
        return format("%s%s", getTypeName(), Arrays.toString(value));
    }

    @Override
    public String getTypeName() {
        return "CharArray";
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE + sizeOf(value);
    }

    @Override
    public boolean hasCompatibleType(AnyValue value) {
        return value instanceof CharValue;
    }

    @Override
    public ArrayValue copyWithAppended(AnyValue added) {
        assert hasCompatibleType(added) : "Incompatible types";
        char[] newArray = Arrays.copyOf(value, value.length + 1);
        newArray[value.length] = ((CharValue) added).value();
        return new CharArray(newArray);
    }

    @Override
    public ArrayValue copyWithPrepended(AnyValue prepended) {
        assert hasCompatibleType(prepended) : "Incompatible types";
        char[] newArray = new char[value.length + 1];
        System.arraycopy(value, 0, newArray, 1, value.length);
        newArray[0] = ((CharValue) prepended).value();
        return new CharArray(newArray);
    }
}
