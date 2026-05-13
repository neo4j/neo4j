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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.VectorCandidate;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;
import org.neo4j.values.virtual.VirtualValues;

class VectorUtilSupportTest {

    @Nested
    class DefaultVectorUtilSupportTest extends TestBase {
        DefaultVectorUtilSupportTest() {
            super(new DefaultVectorUtilSupport());
        }
    }

    @RandomSupportExtension
    abstract static class TestBase {
        static final Percentage CLOSENESS = Percentage.withPercentage(0.1);

        protected final VectorUtilSupport impl;

        @Inject
        private RandomSupport random;

        TestBase(VectorUtilSupport impl) {
            this.impl = impl;
        }

        @RepeatedTest(10)
        void scale() {
            VectorCandidate vector = randomVector(random.random());
            float scale = randomFloatFor(random.random(), vector.dimensions());
            VectorCandidate scaled = impl.scale(vector, scale);
            assertThat(scaled.dimensions()).isEqualTo(vector.dimensions());
            assertThat(scaled).as("vector scaled by " + scale).satisfies(s -> {
                for (int i = 0; i < s.dimensions(); i++) {
                    assertThat(s.floatValue(i)).isCloseTo(scale * vector.floatValue(i), CLOSENESS);
                }
            });
        }

        @ParameterizedTest
        @MethodSource
        void dotProduct(VectorCandidate vector1, VectorCandidate vector2, float expected) {
            float dot = impl.dotProduct(vector1, vector2);
            assertThat(dot).isCloseTo(expected, CLOSENESS);
        }

        static Stream<Arguments> dotProduct() {
            return Stream.of(
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), origin(4), 0.0f),
                    Arguments.of(basisUnitVector(42, 2), basisUnitVector(42, 23), 0.0f),
                    Arguments.of(basisUnitVector(42, 37), basisUnitVector(42, 4), 0.0f),
                    Arguments.of(basisUnitVector(42, 2), basisUnitVector(42, 2), 1.0f),
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), basisUnitVector(4, 0), 1.0f),
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), basisUnitVector(4, 1), 2.0f),
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), basisUnitVector(4, 2), 3.0f),
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), basisUnitVector(4, 3), 4.0f),
                    Arguments.of(vector(-12.0f, 16), vector(12.0f, 9.0f), 0.0f),
                    Arguments.of(vector(-6.0f, 8.0f), vector(5.0f, 12.0f), 66.0f),
                    Arguments.of(vector(9.0f, 2.0f, 7.0f), vector(4.0f, 8.0f, 10.0f), 122.0f));
        }

        @ParameterizedTest
        @MethodSource
        void cosine(VectorCandidate vector1, VectorCandidate vector2, float expected) {
            float cosine = impl.cosine(vector1, vector2);
            assertThat(cosine).isCloseTo(expected, CLOSENESS);
        }

        static Stream<Arguments> cosine() {
            return Stream.of(
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), origin(4), Float.NaN),
                    Arguments.of(basisUnitVector(42, 2), basisUnitVector(42, 23), 0.0f),
                    Arguments.of(basisUnitVector(42, 37), basisUnitVector(42, 4), 0.0f),
                    Arguments.of(basisUnitVector(42, 2), basisUnitVector(42, 2), 1.0f),
                    Arguments.of(vector(1.0f, 0.0f), vector(1.0f, (float) Math.sqrt(3.0f)), 0.5f),
                    Arguments.of(vector(1.0f, 0.0f), vector(1.0f, 1.0f), (float) (Math.sqrt(2.0f) / 2.0f)),
                    Arguments.of(vector(-6.0f, 8.0f), vector(5.0f, 12.0f), (float) Math.cos(Math.toRadians(59.5))));
        }

        @ParameterizedTest
        @MethodSource
        void l1Distance(VectorCandidate vector1, VectorCandidate vector2, float expected) {
            float distance = impl.l1Distance(vector1, vector2);
            assertThat(distance).isCloseTo(expected, CLOSENESS);
        }

        static Stream<Arguments> l1Distance() {
            return Stream.of(
                    Arguments.of(origin(42), origin(42), 0.0f),
                    Arguments.of(basisUnitVector(42, 23), basisUnitVector(42, 23), 0.0f),
                    Arguments.of(basisUnitVector(42, 0), origin(42), 1.0f),
                    Arguments.of(vector(1.0f, 0.0f, 0.0f, 0.0f), origin(4), 1.0f),
                    Arguments.of(vector(1.0f, 1.0f, 0.0f, 0.0f), origin(4), 2.0f),
                    Arguments.of(vector(1.0f, 1.0f, 1.0f, 0.0f), origin(4), 3.0f),
                    Arguments.of(vector(1.0f, 1.0f, 1.0f, 1.0f), origin(4), 4.0f),
                    Arguments.of(vector(4.0f, 0.0f, 0.0f, 0.0f), origin(4), 4.0f),
                    Arguments.of(vector(4.0f, 4.0f, 0.0f, 0.0f), origin(4), 8.0f),
                    Arguments.of(vector(4.0f, 4.0f, 4.0f, 0.0f), origin(4), 12.0f),
                    Arguments.of(vector(4.0f, 4.0f, 4.0f, 4.0f), origin(4), 16.0f),
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), origin(4), 10.0f),
                    Arguments.of(vector(-1.0f, 2.0f, -3.0f, 4.0f), origin(4), 10.0f),
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), vector(-1.0f, 2.0f, -3.0f, 4.0f), 8.0f),
                    Arguments.of(vector(-0.5f, 1.5f, -4.0f, -2.0f), vector(1.0f, 2.25f, -0.75f, 1.125f), 8.625f));
        }

        @ParameterizedTest
        @MethodSource
        void l1Norm(VectorCandidate vector, float expected) {
            float normalization = impl.l1Norm(vector);
            assertThat(normalization).isCloseTo(expected, CLOSENESS);
        }

        static Stream<Arguments> l1Norm() {
            return Stream.of(
                    Arguments.of(origin(42), 0.0f),
                    Arguments.of(basisUnitVector(42, 23), 1.0f),
                    Arguments.of(vector(1.0f, 0.0f, 0.0f, 0.0f), 1.0f),
                    Arguments.of(vector(1.0f, 1.0f, 0.0f, 0.0f), 2.0f),
                    Arguments.of(vector(1.0f, 1.0f, 1.0f, 0.0f), 3.0f),
                    Arguments.of(vector(1.0f, 1.0f, 1.0f, 1.0f), 4.0f),
                    Arguments.of(vector(4.0f, 0.0f, 0.0f, 0.0f), 4.0f),
                    Arguments.of(vector(4.0f, 4.0f, 0.0f, 0.0f), 8.0f),
                    Arguments.of(vector(4.0f, 4.0f, 4.0f, 0.0f), 12.0f),
                    Arguments.of(vector(4.0f, 4.0f, 4.0f, 4.0f), 16.0f),
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), 10.0f),
                    Arguments.of(vector(-1.0f, 2.0f, -3.0f, 4.0f), 10.0f),
                    Arguments.of(vector(1.0f, 2.25f, -0.75f, 1.125f), 5.125f));
        }

        @ParameterizedTest
        @MethodSource
        void squareL2Distance(VectorCandidate vector1, VectorCandidate vector2, float expected) {
            float squareDistance = impl.squareL2Distance(vector1, vector2);
            assertThat(squareDistance).isCloseTo(expected, CLOSENESS);
        }

        static Stream<Arguments> squareL2Distance() {
            return Stream.of(
                    Arguments.of(origin(42), origin(42), 0.0f),
                    Arguments.of(basisUnitVector(42, 23), basisUnitVector(42, 23), 0.0f),
                    Arguments.of(basisUnitVector(42, 0), origin(42), 1.0f),
                    Arguments.of(vector(1.0f, 0.0f, 0.0f, 0.0f), origin(4), 1.0f),
                    Arguments.of(vector(1.0f, 1.0f, 0.0f, 0.0f), origin(4), 2.0f),
                    Arguments.of(vector(1.0f, 1.0f, 1.0f, 0.0f), origin(4), 3.0f),
                    Arguments.of(vector(1.0f, 1.0f, 1.0f, 1.0f), origin(4), 4.0f),
                    Arguments.of(vector(4.0f, 0.0f, 0.0f, 0.0f), origin(4), 16.0f),
                    Arguments.of(vector(4.0f, 4.0f, 0.0f, 0.0f), origin(4), 32.0f),
                    Arguments.of(vector(4.0f, 4.0f, 4.0f, 0.0f), origin(4), 48.0f),
                    Arguments.of(vector(4.0f, 4.0f, 4.0f, 4.0f), origin(4), 64.0f),
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), origin(4), 30.0f),
                    Arguments.of(vector(-1.0f, 2.0f, -3.0f, 4.0f), origin(4), 30.0f),
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), vector(-1.0f, 2.0f, -3.0f, 4.0f), 40.0f),
                    Arguments.of(vector(-0.5f, 1.5f, -4.0f, -2.0f), vector(1.0f, 2.25f, -0.75f, 1.125f), 23.140625f),
                    Arguments.of(vector(16.0f, 23.0f, 42.0f, 55.55f), vector(13.0f, 23.0f, 46.0f, 55.55f), 25.0f));
        }

        @ParameterizedTest
        @MethodSource
        void squareL2Norm(VectorCandidate vector, float expected) {
            float squareDistance = impl.squareL2Norm(vector);
            assertThat(squareDistance).isCloseTo(expected, CLOSENESS);
        }

        static Stream<Arguments> squareL2Norm() {
            return Stream.of(
                    Arguments.of(origin(42), 0.0f),
                    Arguments.of(basisUnitVector(42, 23), 1.0f),
                    Arguments.of(vector(1.0f, 0.0f, 0.0f, 0.0f), 1.0f),
                    Arguments.of(vector(1.0f, 1.0f, 0.0f, 0.0f), 2.0f),
                    Arguments.of(vector(1.0f, 1.0f, 1.0f, 0.0f), 3.0f),
                    Arguments.of(vector(1.0f, 1.0f, 1.0f, 1.0f), 4.0f),
                    Arguments.of(vector(4.0f, 0.0f, 0.0f, 0.0f), 16.0f),
                    Arguments.of(vector(4.0f, 4.0f, 0.0f, 0.0f), 32.0f),
                    Arguments.of(vector(4.0f, 4.0f, 4.0f, 0.0f), 48.0f),
                    Arguments.of(vector(4.0f, 4.0f, 4.0f, 4.0f), 64.0f),
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), 30.0f),
                    Arguments.of(vector(-1.0f, 2.0f, -3.0f, 4.0f), 30.0f),
                    Arguments.of(vector(1.0f, 2.25f, -0.75f, 1.125f), 7.890625f));
        }

        @ParameterizedTest
        @MethodSource
        void hammingDistance(VectorCandidate vector1, VectorCandidate vector2, int expected) {
            int hamming = impl.hammingDistance(vector1, vector2);
            assertThat(hamming).isCloseTo(expected, CLOSENESS);
        }

        static Stream<Arguments> hammingDistance() {
            return Stream.of(
                    Arguments.of(origin(42), origin(42), 0),
                    Arguments.of(basisUnitVector(42, 23), basisUnitVector(42, 23), 0),
                    Arguments.of(basisUnitVector(42, 0), origin(42), 1),
                    Arguments.of(vector(1.0f, 0.0f, 0.0f, 0.0f), origin(4), 1),
                    Arguments.of(vector(1.0f, 1.0f, 0.0f, 0.0f), origin(4), 2),
                    Arguments.of(vector(1.0f, 1.0f, 1.0f, 0.0f), origin(4), 3),
                    Arguments.of(vector(1.0f, 1.0f, 1.0f, 1.0f), origin(4), 4),
                    Arguments.of(vector(4.0f, 0.0f, 0.0f, 0.0f), origin(4), 1),
                    Arguments.of(vector(4.0f, 4.0f, 0.0f, 0.0f), origin(4), 2),
                    Arguments.of(vector(4.0f, 4.0f, 4.0f, 0.0f), origin(4), 3),
                    Arguments.of(vector(4.0f, 4.0f, 4.0f, 4.0f), origin(4), 4),
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), origin(4), 4),
                    Arguments.of(vector(-1.0f, 2.0f, -3.0f, 4.0f), origin(4), 4),
                    Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), vector(-1.0f, 2.0f, -3.0f, 4.0f), 2));
        }
    }

    static float randomFloatFor(Random random, int dimensions) {
        float bound = Math.nextDown((float) (Math.sqrt(Float.MAX_VALUE) / dimensions));
        return random.nextFloat(bound);
    }

    static int randomDimensions(Random random) {
        return random.nextInt(VectorValue.MIN_VECTOR_DIMENSIONS, 1 + VectorValue.MAX_VECTOR_DIMENSIONS);
    }

    static float[] randomRawVector(Random random) {
        return randomRawVector(random, randomDimensions(random));
    }

    static float[] randomRawVector(Random random, int dimensions) {
        boolean origin = true;
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            float element = randomFloatFor(random, dimensions);
            origin &= element == 0.0f;
            vector[i] = element;
        }
        if (origin) {
            int axis = random.nextInt(dimensions);
            float element;
            do {
                element = randomFloatFor(random, dimensions);
            } while (element != 0.0f);
            vector[axis] = element;
        }
        return vector;
    }

    static VectorCandidate vectorCandidate(float... elements) {
        return VectorCandidate.maybeFrom(VirtualValues.list(IntStream.range(0, elements.length)
                .mapToObj(i -> Values.floatValue(elements[i]))
                .toArray(AnyValue[]::new)));
    }

    static VectorCandidate vector(float... elements) {
        return Values.float32Vector(elements);
    }

    static VectorCandidate randomVector(Random random) {
        return vector(randomRawVector(random));
    }

    static VectorCandidate randomVector(Random random, int dimensions) {
        return vector(randomRawVector(random, dimensions));
    }

    static VectorCandidate origin(int dimensions) {
        float[] vector = new float[dimensions];
        Arrays.fill(vector, 0.0f);
        return vector(vector);
    }

    static VectorCandidate basisUnitVector(int dimensions, int basis) {
        float[] vector = new float[dimensions];
        Arrays.fill(vector, 0.0f);
        vector[basis] = 1.0f;
        return vector(vector);
    }
}
