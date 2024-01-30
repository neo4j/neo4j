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
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;

public final class ByteArray extends IntegralArray {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(ByteArray.class);

    private final byte[] value;

    private boolean invalid;

    ByteArray(byte[] value) {
        assert value != null;
        this.value = value;
    }

    private void checkValid() {
        if (invalid) {
            throw new RuntimeException("Invalidated");
        }
    }

    @Override
    public int length() {
        checkValid();
        return value.length;
    }

    @Override
    public long longValue(int index) {
        checkValid();
        return value[index];
    }

    @Override
    public ByteValue value(int offset) {
        checkValid();
        return Values.byteValue(value[offset]);
    }

    @Override
    protected int computeHashToMemoize() {
        checkValid();
        return NumberValues.hash(value);
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        checkValid();
        return mapper.mapByteArray(this);
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
        checkValid();
        return other.equals(value);
    }

    @Override
    public boolean equals(byte[] x) {
        checkValid();
        return Arrays.equals(value, x);
    }

    @Override
    public boolean equals(short[] x) {
        checkValid();
        return PrimitiveArrayValues.equals(value, x);
    }

    @Override
    public boolean equals(int[] x) {
        checkValid();
        return PrimitiveArrayValues.equals(value, x);
    }

    @Override
    public boolean equals(long[] x) {
        checkValid();
        return PrimitiveArrayValues.equals(value, x);
    }

    @Override
    public boolean equals(float[] x) {
        checkValid();
        return PrimitiveArrayValues.equals(value, x);
    }

    @Override
    public boolean equals(double[] x) {
        checkValid();
        return PrimitiveArrayValues.equals(value, x);
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        checkValid();
        writer.writeByteArray(value);
    }

    @Override
    public byte[] asObjectCopy() {
        checkValid();
        return Arrays.copyOf(value, value.length);
    }

    @Override
    @Deprecated
    public byte[] asObject() {
        checkValid();
        return value;
    }

    @Override
    public String prettyPrint() {
        checkValid();
        return Arrays.toString(value);
    }

    @Override
    public String toString() {
        checkValid();
        return format("%s%s", getTypeName(), Arrays.toString(value));
    }

    @Override
    public String getTypeName() {
        return "ByteArray";
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE + sizeOf(value);
    }

    @Override
    public boolean hasCompatibleType(AnyValue value) {
        return value instanceof ByteValue;
    }

    @Override
    public ArrayValue copyWithAppended(AnyValue added) {
        assert hasCompatibleType(added) : "Incompatible types";
        byte[] newArray = Arrays.copyOf(value, value.length + 1);
        newArray[value.length] = ((ByteValue) added).value();
        return new ByteArray(newArray);
    }

    @Override
    public ArrayValue copyWithPrepended(AnyValue prepended) {
        assert hasCompatibleType(prepended) : "Incompatible types";
        byte[] newArray = new byte[value.length + 1];
        System.arraycopy(value, 0, newArray, 1, value.length);
        newArray[0] = ((ByteValue) prepended).value();
        return new ByteArray(newArray);
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return ValueRepresentation.INT8_ARRAY;
    }

    public void zero() {
        invalid = true;
        Arrays.fill(value, (byte) 0);
    }
}
