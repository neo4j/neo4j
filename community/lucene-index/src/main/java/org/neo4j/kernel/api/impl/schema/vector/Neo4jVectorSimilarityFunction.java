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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.apache.lucene.util.VectorUtil.cosine;
import static org.apache.lucene.util.VectorUtil.dotProduct;
import static org.apache.lucene.util.VectorUtil.squareDistance;

import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.values.VectorCandidate;

public enum Neo4jVectorSimilarityFunction implements VectorSimilarityFunction {
    EUCLIDEAN("EUCLIDEAN", "Vector must only contain finite values") {
        @Override
        public float[] maybeToValidVector(VectorCandidate candidate) {
            if (candidate == null || (candidate.dimensions() == 0)) {
                return null;
            }
            int dimensions = candidate.dimensions();

            float[] vector = new float[dimensions];
            for (int i = 0; i < dimensions; i++) {
                float element = candidate.floatValue(i);
                if (!Float.isFinite(element)) {
                    return null;
                }
                vector[i] = element;
            }
            return vector;
        }

        @Override
        public float compare(float[] vector1, float[] vector2) {
            return 1 / (1 + squareDistance(vector1, vector2));
        }
    },
    SIMPLE_COSINE("COSINE", "Vector must only contain finite values, and have positive and finite l2-norm") {
        @Override
        public float[] maybeToValidVector(VectorCandidate candidate) {
            if (candidate == null || (candidate.dimensions() == 0)) {
                return null;
            }
            int dimensions = candidate.dimensions();

            float square = 0.0f;
            float[] vector = new float[dimensions];
            for (int i = 0; i < dimensions; i++) {
                float element = candidate.floatValue(i);
                if (!Float.isFinite(element)) {
                    return null;
                }
                square += element * element;
                vector[i] = element;
            }

            if (square <= 0.0f || !Float.isFinite(square)) {
                return null;
            }

            return vector;
        }

        @Override
        public float compare(float[] vector1, float[] vector2) {
            return Math.max((1 + cosine(vector1, vector2)) / 2, 0);
        }
    },
    L2_NORM_COSINE("COSINE", "Vector must only contain finite values, and have positive and finite l2-norm") {
        @Override
        public float[] maybeToValidVector(VectorCandidate candidate) {
            if (candidate == null || (candidate.dimensions() == 0)) {
                return null;
            }
            int dimensions = candidate.dimensions();

            double square = 0.0;
            for (int i = 0; i < dimensions; i++) {
                double element = candidate.doubleValue(i);
                if (!Double.isFinite(element)) {
                    return null;
                }
                square += element * element;
            }

            double scale;
            if (square <= 0.0 || !Double.isFinite(square) || !Double.isFinite(scale = 1.0 / Math.sqrt(square))) {
                return null;
            }

            float[] vector = new float[dimensions];
            for (int i = 0; i < dimensions; i++) {
                float element = (float) (candidate.doubleValue(i) * scale);
                if (!Float.isFinite(element)) {
                    return null;
                }
                vector[i] = element;
            }

            return vector;
        }

        @Override
        public float compare(float[] vector1, float[] vector2) {
            return Math.max((1 + dotProduct(vector1, vector2)) / 2, 0);
        }
    };

    private final String functionName;
    private final String invalidReason;

    Neo4jVectorSimilarityFunction(String functionName, String invalidReason) {
        this.functionName = functionName;
        this.invalidReason = invalidReason;
    }

    @Override
    public String functionName() {
        return functionName;
    }

    @Override
    public float[] toValidVector(VectorCandidate candidate) {
        float[] vector = maybeToValidVector(candidate);
        if (vector == null) {
            throw InvalidArgumentException.invalidVectorCoordinate(
                    invalidReason + ". Provided: " + candidate, candidate.prettyPrint());
        }
        return vector;
    }
}
