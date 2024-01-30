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

public final class DoubleArray extends FloatingPointArray {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(DoubleArray.class);

    private final double[] value;

    DoubleArray(double[] value) {
        assert value != null;
        this.value = value;
    }

    @Override
    public int length() {
        return value.length;
    }

    @Override
    public boolean hasCompatibleType(AnyValue value) {
        return value instanceof FloatingPointValue;
    }

    @Override
    public float floatValue(int index) {
        return (float) value[index];
    }

    @Override
    public double doubleValue(int index) {
        return value[index];
    }

    @Override
    public DoubleValue value(int position) {
        return Values.doubleValue(doubleValue(position));
    }

    @Override
    protected int computeHashToMemoize() {
        return NumberValues.hash(value);
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapDoubleArray(this);
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
    public boolean equals(byte[] x) {
        return PrimitiveArrayValues.equals(x, value);
    }

    @Override
    public boolean equals(short[] x) {
        return PrimitiveArrayValues.equals(x, value);
    }

    @Override
    public boolean equals(int[] x) {
        return PrimitiveArrayValues.equals(x, value);
    }

    @Override
    public boolean equals(long[] x) {
        return PrimitiveArrayValues.equals(x, value);
    }

    @Override
    public boolean equals(float[] x) {
        return PrimitiveArrayValues.equals(x, value);
    }

    @Override
    public boolean equals(double[] x) {
        return Arrays.equals(x, value);
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        PrimitiveArrayWriting.writeTo(writer, value);
    }

    @Override
    public double[] asObjectCopy() {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    @Deprecated
    public double[] asObject() {
        return value;
    }

    @Override
    public String prettyPrint() {
        return Arrays.toString(value);
    }

    @Override
    public String toString() {
        return format("%s%s", getTypeName(), Arrays.toString(value));
    }

    @Override
    public String getTypeName() {
        return "DoubleArray";
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE + sizeOf(value);
    }

    @Override
    public ArrayValue copyWithAppended(AnyValue added) {
        assert hasCompatibleType(added) : "Incompatible types";
        double[] newArray = Arrays.copyOf(value, value.length + 1);
        newArray[value.length] = ((FloatingPointValue) added).doubleValue();
        return new DoubleArray(newArray);
    }

    @Override
    public ArrayValue copyWithPrepended(AnyValue prepended) {
        assert hasCompatibleType(prepended) : "Incompatible types";
        double[] newArray = new double[value.length + 1];
        System.arraycopy(value, 0, newArray, 1, value.length);
        newArray[0] = ((FloatingPointValue) prepended).doubleValue();
        return new DoubleArray(newArray);
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return ValueRepresentation.FLOAT64_ARRAY;
    }
}
