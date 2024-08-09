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
package org.neo4j.kernel.api.vector;

import static org.neo4j.values.storable.Values.NO_VALUE;

import java.util.Objects;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.NumberArray;
import org.neo4j.values.storable.NumberValue;

public interface VectorCandidate {
    float floatElement(int index);

    double doubleElement(int index);

    int dimensions();

    static VectorCandidate maybeFrom(AnyValue candidate) {
        if (candidate == null || candidate == NO_VALUE) {
            return null;
        }

        if (candidate instanceof final FloatingPointArray floatingPointArray) {
            return new FloatingPointArrayVectorCandidate(floatingPointArray);
        }
        if (candidate instanceof final NumberArray numberArray) {
            return new NumberArrayVectorCandidate(numberArray);
        }
        if (candidate instanceof final SequenceValue sequenceValue) {
            return new SequenceValueVectorCandidate(sequenceValue);
        }

        return null;
    }

    static VectorCandidate from(AnyValue candidate) {
        final var vectorCandidate = maybeFrom(candidate);
        if (vectorCandidate == null) {
            Objects.requireNonNull(candidate, "Value cannot be null");
            throw new IllegalArgumentException("Value is not a valid vector candidate. Provided: " + candidate);
        }
        return vectorCandidate;
    }

    record FloatingPointArrayVectorCandidate(FloatingPointArray array) implements VectorCandidate {

        @Override
        public float floatElement(int index) {
            return array.floatValue(index);
        }

        @Override
        public double doubleElement(int index) {
            return array.doubleValue(index);
        }

        @Override
        public int dimensions() {
            return array.intSize();
        }
    }

    record NumberArrayVectorCandidate(NumberArray array) implements VectorCandidate {

        @Override
        public float floatElement(int index) {
            return array.value(index).floatValue();
        }

        @Override
        public double doubleElement(int index) {
            return array.value(index).doubleValue();
        }

        @Override
        public int dimensions() {
            return array.intSize();
        }
    }

    record SequenceValueVectorCandidate(SequenceValue sequence) implements VectorCandidate {

        @Override
        public float floatElement(int index) {
            return sequence.value(index) instanceof final NumberValue number ? number.floatValue() : Float.NaN;
        }

        @Override
        public double doubleElement(int index) {
            return sequence.value(index) instanceof final NumberValue number ? number.doubleValue() : Double.NaN;
        }

        @Override
        public int dimensions() {
            return sequence.intSize();
        }
    }
}
