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
package org.neo4j.values;

import static org.neo4j.values.storable.Values.NO_VALUE;

import org.neo4j.values.storable.NumberValue;

/**
 * Represents a candidate that might or might not represent a valid vector.
 */
public interface VectorCandidate {

    /**
     * @return the value at the given index as a float. Returns {@code Float.NaN} if the value is not a number (i.e. if this candidate is not valid).
     */
    float floatValue(int index);

    /**
     * @return the value at the given index as a double. Returns {@code Double.NaN} if the value is not a number (i.e. if this candidate is not valid).
     */
    double doubleValue(int index);

    /**
     * @return the number of dimensions in this vector candidate.
     */
    int dimensions();

    /**
     * Like toString but should give the canonical string representation
     */
    String prettyPrint();

    /**
     * @return the canonical representation of the type name
     */
    String getTypeName();

    /**
     * Returns a VectorCandidate if the provided value can be converted to a vector candidate, otherwise null.
     */
    static VectorCandidate maybeFrom(AnyValue candidate) {
        if (candidate == null || candidate == NO_VALUE) {
            return null;
        }

        return switch (candidate) {
            case VectorCandidate vectorCandidate -> vectorCandidate;
            case SequenceValue sequenceValue -> new SequenceValueVectorCandidate(sequenceValue);
            default -> null;
        };
    }

    /**
     * A VectorCandidate wrapping a SequenceValue. This candidate may or may not be valid, depending on the
     * contents of the sequence.
     */
    record SequenceValueVectorCandidate(SequenceValue sequence) implements VectorCandidate {

        @Override
        public float floatValue(int index) {
            return sequence.value(index) instanceof NumberValue number ? number.floatValue() : Float.NaN;
        }

        @Override
        public double doubleValue(int index) {
            return sequence.value(index) instanceof NumberValue number ? number.doubleValue() : Double.NaN;
        }

        @Override
        public int dimensions() {
            return sequence.intSize();
        }

        @Override
        public String toString() {
            return sequence.toString();
        }

        @Override
        public String prettyPrint() {
            return sequence.prettyPrint();
        }

        @Override
        public String getTypeName() {
            return sequence.getTypeName();
        }
    }
}
