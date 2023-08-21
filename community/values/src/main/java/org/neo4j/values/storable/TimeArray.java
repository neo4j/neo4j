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

import static org.neo4j.memory.HeapEstimator.OFFSET_TIME_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.sizeOfObjectArray;

import java.time.OffsetTime;
import java.util.Arrays;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;

public class TimeArray extends TemporalArray<OffsetTime> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(TimeArray.class);

    private final OffsetTime[] value;

    TimeArray(OffsetTime[] value) {
        assert value != null;
        this.value = value;
    }

    @Override
    protected OffsetTime[] value() {
        return value;
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapTimeArray(this);
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
    public boolean equals(OffsetTime[] x) {
        return Arrays.equals(value, x);
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writeTo(writer, ValueWriter.ArrayType.ZONED_TIME, value);
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return ValueRepresentation.ZONED_TIME_ARRAY;
    }

    @Override
    protected int unsafeCompareTo(Value otherValue) {
        return compareToNonPrimitiveArray((TimeArray) otherValue);
    }

    @Override
    public String getTypeName() {
        return "TimeArray";
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE + sizeOfObjectArray(OFFSET_TIME_SIZE, value.length);
    }

    @Override
    public boolean hasCompatibleType(AnyValue value) {
        return value instanceof TimeValue;
    }

    @Override
    public ArrayValue copyWithAppended(AnyValue added) {
        assert hasCompatibleType(added) : "Incompatible types";
        OffsetTime[] newArray = Arrays.copyOf(value, value.length + 1);
        newArray[value.length] = ((TimeValue) added).temporal();
        return new TimeArray(newArray);
    }

    @Override
    public ArrayValue copyWithPrepended(AnyValue prepended) {
        assert hasCompatibleType(prepended) : "Incompatible types";
        OffsetTime[] newArray = new OffsetTime[value.length + 1];
        System.arraycopy(value, 0, newArray, 1, value.length);
        newArray[0] = ((TimeValue) prepended).temporal();
        return new TimeArray(newArray);
    }
}
