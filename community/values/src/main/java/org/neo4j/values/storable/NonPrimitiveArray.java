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

import java.util.Arrays;
import org.neo4j.hashing.HashFunction;

public abstract class NonPrimitiveArray<T extends Comparable<? super T>> extends ArrayValue {
    protected abstract T[] value();

    @Override
    public final boolean equals(boolean[] x) {
        return false;
    }

    @Override
    public final boolean equals(char[] x) {
        return false;
    }

    @Override
    public final boolean equals(String[] x) {
        return false;
    }

    @Override
    public final boolean equals(byte[] x) {
        return false;
    }

    @Override
    public final boolean equals(short[] x) {
        return false;
    }

    @Override
    public final boolean equals(int[] x) {
        return false;
    }

    @Override
    public final boolean equals(long[] x) {
        return false;
    }

    @Override
    public final boolean equals(float[] x) {
        return false;
    }

    @Override
    public final boolean equals(double[] x) {
        return false;
    }

    @Override
    public final NumberType numberType() {
        return NumberType.NO_NUMBER;
    }

    final int compareToNonPrimitiveArray(NonPrimitiveArray<T> other) {
        int compare = 0;
        int length = Math.min(this.intSize(), other.intSize());
        for (int index = 0; compare == 0 && index < length; index++) {
            compare = this.value()[index].compareTo(other.value()[index]);
        }
        return compare == 0 ? Integer.compare(this.intSize(), other.intSize()) : compare;
    }

    @Override
    protected int computeHashToMemoize() {
        return Arrays.hashCode(value());
    }

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        hash = hashFunction.update(hash, this.intSize());
        for (T obj : value()) {
            hash = hashFunction.update(hash, obj.hashCode());
        }
        return hash;
    }

    @Override
    public final int intSize() {
        return value().length;
    }

    @Override
    public final T[] asObjectCopy() {
        T[] value = value();
        return Arrays.copyOf(value, value.length);
    }

    @Override
    @Deprecated
    public final T[] asObject() {
        return value();
    }

    @Override
    public final String prettyPrint() {
        return Arrays.toString(value());
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + Arrays.toString(value());
    }
}
