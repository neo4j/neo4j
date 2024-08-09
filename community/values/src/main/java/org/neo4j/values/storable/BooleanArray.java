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

public final class BooleanArray extends ArrayValue {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(BooleanArray.class);

    private final boolean[] value;

    BooleanArray(boolean[] value) {
        assert value != null;
        this.value = value;
    }

    @Override
    public int intSize() {
        return value.length;
    }

    public boolean booleanValue(int offset) {
        return value[offset];
    }

    @Override
    public String getTypeName() {
        return "BooleanArray";
    }

    @Override
    public boolean equals(Value other) {
        return other.equals(this.value);
    }

    @Override
    public boolean equals(boolean[] x) {
        return Arrays.equals(value, x);
    }

    @Override
    protected int computeHashToMemoize() {
        return NumberValues.hash(value);
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        hash = hashFunction.update(hash, value.length);
        hash = hashFunction.update(hash, hashCode());
        return hash;
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapBooleanArray(this);
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        PrimitiveArrayWriting.writeTo(writer, value);
    }

    @Override
    public boolean[] asObjectCopy() {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    @Deprecated
    public boolean[] asObject() {
        return value;
    }

    @Override
    protected int unsafeCompareTo(Value otherValue) {
        return NumberValues.compareBooleanArrays(this, (BooleanArray) otherValue);
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return ValueRepresentation.BOOLEAN_ARRAY;
    }

    @Override
    public NumberType numberType() {
        return NumberType.NO_NUMBER;
    }

    @Override
    public String prettyPrint() {
        return Arrays.toString(value);
    }

    @Override
    public AnyValue value(int position) {
        return Values.booleanValue(booleanValue(position));
    }

    @Override
    public String toString() {
        return format("%s%s", getTypeName(), Arrays.toString(value));
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE + sizeOf(value);
    }

    @Override
    public boolean hasCompatibleType(AnyValue value) {
        return value instanceof BooleanValue;
    }

    @Override
    public ArrayValue copyWithAppended(AnyValue added) {
        assert hasCompatibleType(added) : "Incompatible types";
        boolean[] newArray = Arrays.copyOf(value, value.length + 1);
        newArray[value.length] = ((BooleanValue) added).booleanValue();
        return new BooleanArray(newArray);
    }

    @Override
    public ArrayValue copyWithPrepended(AnyValue prepended) {
        assert hasCompatibleType(prepended) : "Incompatible types";
        boolean[] newArray = new boolean[value.length + 1];
        newArray[0] = ((BooleanValue) prepended).booleanValue();
        System.arraycopy(value, 0, newArray, 1, value.length);
        return new BooleanArray(newArray);
    }

    @Override
    public boolean equals(SequenceValue other) {
        if (other instanceof ArrayValue otherArray) {
            return otherArray.equals(value);
        } else {
            return super.equals(other);
        }
    }
}
