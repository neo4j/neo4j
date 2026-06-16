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

import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.graphdb.Vector;
import org.neo4j.hashing.HashFunction;
import org.neo4j.values.Comparison;
import org.neo4j.values.VectorCandidate;
import org.neo4j.values.utils.PrettyPrinter;

public abstract sealed class VectorValue extends HashMemoizingScalarValue
        implements Vector, VectorCandidate, Comparable<VectorValue> permits IntegralVector, FloatingPointVector {

    public static final int MIN_VECTOR_DIMENSIONS = 1;
    public static final int MAX_VECTOR_DIMENSIONS = 4096;

    @Override
    public Vector asObjectCopy() {
        // Similar to PointValue, all VectorValues implement the public interface Vector,
        // which is the Java representation.
        return this;
    }

    @Override
    public String prettyPrint() {
        PrettyPrinter pp = new PrettyPrinter();
        writeTo(pp);
        return pp.value();
    }

    @Override
    public Comparison unsafeTernaryCompareTo(Value otherValue) {
        // Vector values are not comparable under Comparability semantics,
        // unless they are equal.
        if (equals(otherValue)) {
            return Comparison.EQUAL;
        } else {
            return Comparison.UNDEFINED;
        }
    }

    @Override
    public int compareTo(VectorValue that) {
        return Values.COMPARATOR.compare(this, that);
    }

    @Override
    public boolean isIncomparableType() {
        return true;
    }

    /**
     * In order to facilitate implementing {@link #updateHash(HashFunction, long)}.
     *
     * @param i the index of the coordinate
     * @return the long bit representation of the coordinate.
     */
    protected abstract long longBits(int i);

    public abstract String nestedTypeName();

    @Override
    public long updateHash(HashFunction hashFunction, long hash) {
        int len = dimensions();
        hash = hashFunction.update(hash, len);
        for (int i = 0; i < len; i++) {
            hash = hashFunction.update(hash, longBits(i));
        }
        return hash;
    }

    public static void ensureValidDimensions(int dimensions) {
        if (dimensions < VectorValue.MIN_VECTOR_DIMENSIONS || dimensions > VectorValue.MAX_VECTOR_DIMENSIONS) {
            throw InvalidArgumentException.invalidVectorDimensions(
                    VectorValue.MIN_VECTOR_DIMENSIONS, VectorValue.MAX_VECTOR_DIMENSIONS, dimensions);
        }
    }

    public static void ensureFiniteCoordinates(float[] coordinates) {
        for (float c : coordinates) {
            if (!Float.isFinite(c)) {
                throw InvalidArgumentException.invalidVectorCoordinate(coordinates);
            }
        }
    }

    public static void ensureFiniteCoordinates(double[] coordinates) {
        for (double c : coordinates) {
            if (!Double.isFinite(c)) {
                throw InvalidArgumentException.invalidVectorCoordinate(coordinates);
            }
        }
    }
}
