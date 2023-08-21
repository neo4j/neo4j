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

import static org.neo4j.memory.HeapEstimator.LOCAL_DATE_TIME_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.sizeOfObjectArray;

import java.time.LocalDateTime;
import java.util.Arrays;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;

public class LocalDateTimeArray extends TemporalArray<LocalDateTime> {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(LocalDateTimeArray.class);

    private final LocalDateTime[] value;

    LocalDateTimeArray(LocalDateTime[] value) {
        assert value != null;
        this.value = value;
    }

    @Override
    protected LocalDateTime[] value() {
        return value;
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return mapper.mapLocalDateTimeArray(this);
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
    public boolean equals(LocalDateTime[] x) {
        return Arrays.equals(value, x);
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {
        writeTo(writer, ValueWriter.ArrayType.LOCAL_DATE_TIME, value);
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return ValueRepresentation.LOCAL_DATE_TIME_ARRAY;
    }

    @Override
    protected int unsafeCompareTo(Value otherValue) {
        return compareToNonPrimitiveArray((LocalDateTimeArray) otherValue);
    }

    @Override
    public String getTypeName() {
        return "LocalDateTimeArray";
    }

    @Override
    public long estimatedHeapUsage() {
        return SHALLOW_SIZE + sizeOfObjectArray(LOCAL_DATE_TIME_SIZE, value.length);
    }

    @Override
    public boolean hasCompatibleType(AnyValue value) {
        return value instanceof LocalDateTimeValue;
    }

    @Override
    public ArrayValue copyWithAppended(AnyValue added) {
        assert hasCompatibleType(added) : "Incompatible types";
        LocalDateTime[] newArray = Arrays.copyOf(value, value.length + 1);
        newArray[value.length] = ((LocalDateTimeValue) added).temporal();
        return new LocalDateTimeArray(newArray);
    }

    @Override
    public ArrayValue copyWithPrepended(AnyValue prepended) {
        assert hasCompatibleType(prepended) : "Incompatible types";
        LocalDateTime[] newArray = new LocalDateTime[value.length + 1];
        System.arraycopy(value, 0, newArray, 1, value.length);
        newArray[0] = ((LocalDateTimeValue) prepended).temporal();
        return new LocalDateTimeArray(newArray);
    }
}
