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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.neo4j.hashing.HashFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;

/**
 * Array of one of the storable primitives
 */
public abstract class ArrayValue extends HashMemoizingValue implements SequenceValue {

    @Override
    public long actualSize() {
        return intSize();
    }

    @Override
    protected int computeHashToMemoize() {
        return 0;
    }

    @Override
    public boolean equals(Value other) {
        return false;
    }

    @Override
    protected int unsafeCompareTo(Value other) {
        return 0;
    }

    @Override
    public <E extends Exception> void writeTo(ValueWriter<E> writer) throws E {}

    @Override
    public Object asObjectCopy() {
        return null;
    }

    @Override
    public String prettyPrint() {
        return "";
    }

    @Override
    public ValueRepresentation valueRepresentation() {
        return null;
    }

    @Override
    public NumberType numberType() {
        return null;
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        return 0;
    }

    @Override
    public <T> T map(ValueMapper<T> mapper) {
        return null;
    }

    @Override
    public String getTypeName() {
        return "";
    }

    @Override
    public long estimatedHeapUsage() {
        return 0;
    }

    public abstract boolean hasCompatibleType(AnyValue value);

    public abstract ArrayValue copyWithAppended(AnyValue added);

    public abstract ArrayValue copyWithPrepended(AnyValue prepended);

    public abstract AnyValue value(int offset);

    @Override
    public IterationPreference iterationPreference() {
        return IterationPreference.RANDOM_ACCESS;
    }

    @Override
    public Iterator<AnyValue> iterator() {
        return new Iterator<>() {
            private int offset;

            @Override
            public boolean hasNext() {
                return offset < intSize();
            }

            @Override
            public AnyValue next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return value(offset++);
            }
        };
    }

    @Override
    public final boolean equalTo(Object other) {
        if (other == null) {
            return false;
        }

        return other instanceof SequenceValue && this.equals((SequenceValue) other);
    }

    @Override
    public AnyValue value(long offset) {
        Objects.checkIndex(offset, intSize());
        return value((int) offset);
    }

    @Override
    public final boolean equals(boolean x) {
        return false;
    }

    @Override
    public final boolean equals(long x) {
        return false;
    }

    @Override
    public final boolean equals(double x) {
        return false;
    }

    @Override
    public final boolean equals(char x) {
        return false;
    }

    @Override
    public final boolean equals(String x) {
        return false;
    }

    @Override
    public boolean isSequenceValue() {
        return true;
    }
}
