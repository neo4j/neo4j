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

import org.neo4j.values.VectorCandidate;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;

// todo: consider unrolling in blocks of 8 and FMA for this fallback implementation
//       On newer CPUs `Math.fma(a, b, c)` is faster than `a * b + c` and more accurate;
//       however, not all CPUs have FMA  instructions, and the Java fallback is BigDecimal in which case FMA will be
//       slower! Lucene does extra checks to see if "fast FMA" is available and uses it.

/**
 * Default {@link VectorUtilSupport} using simple CPU instructions
 */
class DefaultVectorUtilSupport implements VectorUtilSupport {

    @Override
    public VectorValue scale(VectorCandidate vector, float scale) {
        int dimensions = vector.dimensions();
        float[] scaled = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            scaled[i] = scale * vector.floatValue(i);
        }
        return Values.float32Vector(scaled);
    }

    @Override
    public float dotProduct(VectorCandidate vector1, VectorCandidate vector2) {
        int dimensions = vector1.dimensions();
        float sum = 0.0f;
        for (int i = 0; i < dimensions; i++) {
            sum += vector1.floatValue(i) * vector2.floatValue(i);
        }
        return sum;
    }

    @Override
    public float cosine(VectorCandidate vector1, VectorCandidate vector2) {
        int dimensions = vector1.dimensions();
        float norm1 = 0.0f;
        float norm2 = 0.0f;
        float sum = 0.0f;
        for (int i = 0; i < dimensions; i++) {
            float element1 = vector1.floatValue(i);
            float element2 = vector2.floatValue(i);
            norm1 += element1 * element1;
            norm2 += element2 * element2;
            sum += element1 * element2;
        }
        return (float) (sum / Math.sqrt(norm1 * norm2));
    }

    @Override
    public float l1Distance(VectorCandidate vector1, VectorCandidate vector2) {
        int dimensions = vector1.dimensions();
        float sum = 0.0f;
        for (int i = 0; i < dimensions; i++) {
            float diff = vector1.floatValue(i) - vector2.floatValue(i);
            sum += Math.abs(diff);
        }
        return sum;
    }

    @Override
    public float l1Norm(VectorCandidate vector) {
        int dimensions = vector.dimensions();
        float sum = 0.0f;
        for (int i = 0; i < dimensions; i++) {
            float element = vector.floatValue(i);
            sum += Math.abs(element);
        }
        return sum;
    }

    @Override
    public float squareL2Distance(VectorCandidate vector1, VectorCandidate vector2) {
        int dimensions = vector1.dimensions();
        float square = 0.0f;
        for (int i = 0; i < dimensions; i++) {
            float diff = vector1.floatValue(i) - vector2.floatValue(i);
            square += diff * diff;
        }
        return square;
    }

    @Override
    public float squareL2Norm(VectorCandidate vector) {
        int dimensions = vector.dimensions();
        float sum = 0.0f;
        for (int i = 0; i < dimensions; i++) {
            float element = vector.floatValue(i);
            sum += element * element;
        }
        return sum;
    }

    // todo: Hamming distance on floating point values is odd
    //       should consider if we should allow an IntegralVectorCandidate to specialise integral operations
    @Override
    public int hammingDistance(VectorCandidate vector1, VectorCandidate vector2) {
        int dimensions = vector1.dimensions();
        int diff = 0;
        for (int i = 0; i < dimensions; i++) {
            if (vector1.floatValue(i) != vector2.floatValue(i)) {
                diff++;
            }
        }
        return diff;
    }
}
