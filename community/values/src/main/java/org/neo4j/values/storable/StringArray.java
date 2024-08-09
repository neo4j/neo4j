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
import static org.neo4j.memory.HeapEstimator.sizeOfObjectArray;
import static org.neo4j.values.storable.NoValue.NO_VALUE;
import static org.neo4j.values.utils.ValueMath.HASH_CONSTANT;

import java.util.Arrays;
import org.neo4j.hashing.HashFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;

public class StringArray extends TextArray {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(StringArray.class);

    private final String[] value;

    StringArray(String[] value) {
        assert value != null;
        this.value = value;
    }

    @Override
    public int intSize() {
        return value.length;
    }

    @Override
    public String stringValue(int offset) {
        return value[offset];
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
        return PrimitiveArrayValues.equals(x, value);
    }

    @Override
    public boolean equals(String[] x) {
        return Arrays.equals(value, x);
    }

    @Override
    protected int computeHashToMemoize() {
        int result = 1;
        for (String element : value) {
            result = HASH_CONSTANT * result
                    + (element == null
                            ? NO_VALUE.hashCode()
                            : Values.stringValue(element).hashCode());
        }
        return result;
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        hash = hashFunction.update(hash, value.length);
        for (String s : value) {
            hash = StringWrappingStringValue.updateHash(hashFunction, hash, s);
        }
        return hash;
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        PrimitiveArrayWriting.writeTo(writer, value);
    }

    @Override
    public String[] asObjectCopy() {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    @Deprecated
    public String[] asObject() {
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
    public Value value(int offset) {
        return Values.stringOrNoValue(stringValue(offset));
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapStringArray(this);
    }

    @Override
    public String toString() {
        return format("%s%s", getTypeName(), Arrays.toString(value));
    }

    @Override
    public String getTypeName() {
        return "StringArray";
    }

    @Override
    public long estimatedHeapUsage() {
        int length = value.length;
        return SHALLOW_SIZE
                + (length == 0 ? 0 : sizeOfObjectArray(sizeOf(value[0]), length)); // Use first element as probe
    }

    @Override
    public boolean hasCompatibleType(AnyValue value) {
        return value instanceof TextValue;
    }

    @Override
    public ArrayValue copyWithAppended(AnyValue added) {
        assert hasCompatibleType(added) : "Incompatible types";
        String[] newArray = Arrays.copyOf(value, value.length + 1);
        newArray[value.length] = ((TextValue) added).stringValue();
        return new StringArray(newArray);
    }

    @Override
    public ArrayValue copyWithPrepended(AnyValue prepended) {
        assert hasCompatibleType(prepended) : "Incompatible types";
        String[] newArray = new String[value.length + 1];
        System.arraycopy(value, 0, newArray, 1, value.length);
        newArray[0] = ((TextValue) prepended).stringValue();
        return new StringArray(newArray);
    }
}
