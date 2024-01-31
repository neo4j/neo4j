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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.assertj.core.data.Percentage;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.values.AnyValue;

class VectorSimilarityFunctionsTest {

    @TestInstance(Lifecycle.PER_CLASS)
    abstract static class TestBase {
        private final VectorSimilarityFunction similarityFunction;

        private TestBase(VectorSimilarityFunction similarityFunction) {
            this.similarityFunction = similarityFunction;
        }

        abstract RichIterable<AnyValue> invalidVectors();

        @ParameterizedTest
        @MethodSource
        void invalidVectors(AnyValue candidate) {
            assertThat(similarityFunction.maybeToValidVector(candidate))
                    .as("invalid vector candidate should return null")
                    .isNull();
        }

        abstract RichIterable<AnyValue> validVectors();

        @ParameterizedTest
        @MethodSource
        void validVectors(AnyValue candidate) {
            assertThat(similarityFunction.maybeToValidVector(candidate))
                    .as("valid vector candidate should return value")
                    .isNotNull();
        }

        @ParameterizedTest
        @MethodSource
        void validPairs(float[] lhs, float[] rhs) {
            // temporary measure
            final var luceneVectorUtilClass = org.apache.lucene.util.VectorUtil.class;
            luceneVectorUtilClass.getClassLoader().setClassAssertionStatus(luceneVectorUtilClass.getName(), false);

            final var ref = new MutableDouble();

            // lhs vs rhs
            assertThatCode(() -> ref.setValue(similarityFunction.compare(lhs, rhs)))
                    .as("valid pairs of vectors should be comparable")
                    .doesNotThrowAnyException();

            final var score = ref.doubleValue();
            assertThat(Double.isFinite(score)).as("score should be finite").isTrue();

            // rhs vs lhs
            assertThatCode(() -> ref.setValue(similarityFunction.compare(rhs, lhs)))
                    .as("valid pairs of vectors should be comparable")
                    .doesNotThrowAnyException();

            final var commutativeScore = ref.doubleValue();
            assertThat(Double.isFinite(commutativeScore))
                    .as("score should be finite")
                    .isTrue();

            // should be 'equal'
            assertThat(commutativeScore).isCloseTo(score, Percentage.withPercentage(1e-12));
        }

        Iterable<Arguments> validPairs() {
            final var vectors = validVectors()
                    .asLazy()
                    .collect(similarityFunction::maybeToValidVector)
                    .select(Objects::nonNull) // sanity check
                    .toSortedSet(Comparator.<float[]>comparingInt(array -> array.length)
                            .thenComparing(Arrays::compare))
                    .toList();

            final var pairs = Lists.mutable.<Arguments>empty();
            final var numberOfVectors = vectors.size();
            for (int i = 0; i < numberOfVectors; i++) {
                final var lhs = vectors.get(i);
                for (int j = i; j < numberOfVectors; j++) {
                    final var rhs = vectors.get(j);
                    if (lhs.length != rhs.length) {
                        // exhausted these dimensional vectors
                        break;
                    }

                    pairs.add(Arguments.of(lhs, rhs));
                }
            }

            return pairs;
        }
    }

    @Nested
    public final class Euclidean extends TestBase {

        public Euclidean() {
            super(VectorSimilarityFunctions.EUCLIDEAN);
        }

        @Override
        RichIterable<AnyValue> invalidVectors() {
            return VectorTestUtils.EUCLIDEAN_INVALID_VECTORS;
        }

        @Override
        RichIterable<AnyValue> validVectors() {
            return VectorTestUtils.EUCLIDEAN_VALID_VECTORS;
        }
    }

    @Nested
    public final class SimpleCosine extends TestBase {

        public SimpleCosine() {
            super(VectorSimilarityFunctions.SIMPLE_COSINE);
        }

        @Override
        RichIterable<AnyValue> invalidVectors() {
            return VectorTestUtils.SIMPLE_COSINE_INVALID_VECTORS;
        }

        @Override
        RichIterable<AnyValue> validVectors() {
            return VectorTestUtils.SIMPLE_COSINE_VALID_VECTORS;
        }
    }

    @Nested
    public final class L2NormCosine extends TestBase {

        public L2NormCosine() {
            super(VectorSimilarityFunctions.L2_NORM_COSINE);
        }

        @Override
        RichIterable<AnyValue> invalidVectors() {
            return VectorTestUtils.L2_NORM_COSINE_INVALID_VECTORS;
        }

        @Override
        RichIterable<AnyValue> validVectors() {
            return VectorTestUtils.L2_NORM_COSINE_VALID_VECTORS;
        }
    }
}
