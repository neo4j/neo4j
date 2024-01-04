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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.values.storable.Value;

class VectorTestUtilsTest {

    @TestInstance(Lifecycle.PER_CLASS)
    abstract static class TestBase {
        private final VectorSimilarityFunction similarityFunction;

        private TestBase(VectorSimilarityFunction similarityFunction) {
            this.similarityFunction = similarityFunction;
        }

        abstract Iterable<Value> validVectorsFromValue();

        @ParameterizedTest
        @MethodSource
        void validVectorsFromValue(Value candidate) {
            assertThat(similarityFunction.maybeToValidVector(candidate))
                    .as("valid vector candidate should return value")
                    .isNotNull();
        }

        abstract Iterable<Value> invalidVectorsFromValue();

        @ParameterizedTest
        @MethodSource
        void invalidVectorsFromValue(Value candidate) {
            assertThat(similarityFunction.maybeToValidVector(candidate))
                    .as("invalid vector candidate should return null")
                    .isNull();
        }

        abstract Iterable<List<Double>> validVectorsFromDoubleList();

        @ParameterizedTest
        @MethodSource
        void validVectorsFromDoubleList(List<Double> candidate) {
            assertThat(similarityFunction.maybeToValidVector(candidate))
                    .as("valid vector candidate should return value")
                    .isNotNull();
        }

        abstract Iterable<List<Double>> invalidVectorsFromDoubleList();

        @ParameterizedTest
        @MethodSource
        void invalidVectorsFromDoubleList(List<Double> candidate) {
            assertThat(similarityFunction.maybeToValidVector(candidate))
                    .as("invalid vector candidate should return null")
                    .isNull();
        }
    }

    @Nested
    public final class Euclidean extends TestBase {

        public Euclidean() {
            super(VectorSimilarityFunctions.EUCLIDEAN);
        }

        @Override
        Iterable<Value> validVectorsFromValue() {
            return VectorTestUtils.EUCLIDEAN_VALID_VECTORS_FROM_VALUE;
        }

        @Override
        Iterable<Value> invalidVectorsFromValue() {
            return VectorTestUtils.EUCLIDEAN_INVALID_VECTORS_FROM_VALUE;
        }

        @Override
        Iterable<List<Double>> validVectorsFromDoubleList() {
            return VectorTestUtils.EUCLIDEAN_VALID_VECTORS_FROM_DOUBLE_LIST;
        }

        @Override
        Iterable<List<Double>> invalidVectorsFromDoubleList() {
            return VectorTestUtils.EUCLIDEAN_INVALID_VECTORS_FROM_DOUBLE_LIST;
        }
    }

    @Nested
    public final class Cosine extends TestBase {

        public Cosine() {
            super(VectorSimilarityFunctions.COSINE);
        }

        @Override
        Iterable<Value> validVectorsFromValue() {
            return VectorTestUtils.COSINE_VALID_VECTORS_FROM_VALUE;
        }

        @Override
        Iterable<Value> invalidVectorsFromValue() {
            return VectorTestUtils.COSINE_INVALID_VECTORS_FROM_VALUE;
        }

        @Override
        Iterable<List<Double>> validVectorsFromDoubleList() {
            return VectorTestUtils.COSINE_VALID_VECTORS_FROM_DOUBLE_LIST;
        }

        @Override
        Iterable<List<Double>> invalidVectorsFromDoubleList() {
            return VectorTestUtils.COSINE_INVALID_VECTORS_FROM_DOUBLE_LIST;
        }
    }
}
