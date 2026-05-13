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
import static org.eclipse.collections.impl.tuple.Tuples.pair;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.vector.VectorUtilSupportTest.TestBase.CLOSENESS;
import static org.neo4j.kernel.api.vector.VectorUtilSupportTest.basisUnitVector;
import static org.neo4j.kernel.api.vector.VectorUtilSupportTest.origin;
import static org.neo4j.kernel.api.vector.VectorUtilSupportTest.randomDimensions;
import static org.neo4j.kernel.api.vector.VectorUtilSupportTest.randomVector;
import static org.neo4j.kernel.api.vector.VectorUtilSupportTest.vector;
import static org.neo4j.kernel.api.vector.VectorUtilSupportTest.vectorCandidate;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.eclipse.collections.api.tuple.Pair;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssert;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectAssertions;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.values.VectorCandidate;

/**
 * Functions which are simply wrappers around {@link VectorUtilSupport} method are tested in
 * {@link VectorUtilSupportTest} rather than here.
 */
@RandomSupportExtension
class VectorUtilTest {

    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @MethodSource("l2Norm")
    void magnitude(VectorCandidate vector, float expected) {
        float magnitude = VectorUtil.magnitude(vector);
        assertThat(magnitude).isCloseTo(expected, CLOSENESS);
    }

    @ParameterizedTest
    @MethodSource
    void l1NormalizedVector(VectorCandidate vector, VectorCandidate expected) {
        VectorCandidate normalized = VectorUtil.l1NormalizedVector(vector);
        assertThat(normalized.dimensions()).isEqualTo(vector.dimensions()).isEqualTo(expected.dimensions());
        assertThat(normalized).satisfies(n -> {
            for (int i = 0; i < normalized.dimensions(); i++) {
                assertThat(normalized.floatValue(i)).isCloseTo(expected.floatValue(i), CLOSENESS);
            }
        });
    }

    static Stream<Arguments> l1NormalizedVector() {
        return Stream.of(
                Arguments.of(vector(1.0f, 0.0f, 0.0f, 0.0f), vector(1.0f, 0.0f, 0.0f, 0.0f)),
                Arguments.of(vector(1.0f, 1.0f, 0.0f, 0.0f), vector(0.5f, 0.5f, 0.0f, 0.0f)),
                Arguments.of(vector(1.0f, 1.0f, 1.0f, 0.0f), vector(1.0f / 3.0f, 1.0f / 3.0f, 1.0f / 3.0f, 0.0f)),
                Arguments.of(vector(1.0f, 1.0f, 1.0f, 1.0f), vector(0.25f, 0.25f, 0.25f, 0.25f)),
                Arguments.of(vector(4.0f, 0.0f, 0.0f, 0.0f), vector(1.0f, 0.0f, 0.0f, 0.0f)),
                Arguments.of(vector(4.0f, 4.0f, 0.0f, 0.0f), vector(0.5f, 0.5f, 0.0f, 0.0f)),
                Arguments.of(vector(4.0f, 4.0f, 4.0f, 0.0f), vector(1.0f / 3.0f, 1.0f / 3.0f, 1.0f / 3.0f, 0.0f)),
                Arguments.of(vector(4.0f, 4.0f, 4.0f, 4.0f), vector(0.25f, 0.25f, 0.25f, 0.25f)),
                Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), vector(0.1f, 0.2f, 0.3f, 0.4f)),
                Arguments.of(vector(-1.0f, 2.0f, -3.0f, 4.0f), vector(-0.1f, 0.2f, -0.3f, 0.4f)));
    }

    @ParameterizedTest
    @MethodSource
    void l2Distance(VectorCandidate vector1, VectorCandidate vector2, float expected) {
        float distance = VectorUtil.l2Distance(vector1, vector2);
        assertThat(distance).isCloseTo(expected, CLOSENESS);
    }

    static Stream<Arguments> l2Distance() {
        return Stream.of(
                Arguments.of(origin(42), origin(42), 0.0f),
                Arguments.of(basisUnitVector(42, 23), basisUnitVector(42, 23), 0.0f),
                Arguments.of(basisUnitVector(42, 0), origin(42), 1.0f),
                Arguments.of(vector(1.0f, 0.0f, 0.0f, 0.0f), origin(4), 1.0f),
                Arguments.of(vector(1.0f, 1.0f, 0.0f, 0.0f), origin(4), (float) Math.sqrt(2.0)),
                Arguments.of(vector(1.0f, 1.0f, 1.0f, 0.0f), origin(4), (float) Math.sqrt(3.0)),
                Arguments.of(vector(1.0f, 1.0f, 1.0f, 1.0f), origin(4), 2.0f),
                Arguments.of(vector(4.0f, 0.0f, 0.0f, 0.0f), origin(4), 4.0f),
                Arguments.of(vector(4.0f, 4.0f, 0.0f, 0.0f), origin(4), 4.0f * (float) Math.sqrt(2.0)),
                Arguments.of(vector(4.0f, 4.0f, 4.0f, 0.0f), origin(4), 4.0f * (float) Math.sqrt(3.0)),
                Arguments.of(vector(4.0f, 4.0f, 4.0f, 4.0f), origin(4), 8.0f),
                Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), origin(4), (float) Math.sqrt(30.0)),
                Arguments.of(vector(-1.0f, 2.0f, -3.0f, 4.0f), origin(4), (float) Math.sqrt(30.0)),
                Arguments.of(
                        vector(1.0f, 2.0f, 3.0f, 4.0f),
                        vector(-1.0f, 2.0f, -3.0f, 4.0f),
                        2.0f * (float) Math.sqrt(10.0)),
                Arguments.of(vector(-0.5f, 1.5f, -4.0f, -2.0f), vector(1.0f, 2.25f, -0.75f, 1.125f), (float)
                        Math.sqrt(23.140625)),
                Arguments.of(vector(16.0f, 23.0f, 42.0f, 55.55f), vector(13.0f, 23.0f, 46.0f, 55.55f), 5.0f));
    }

    @ParameterizedTest
    @MethodSource
    void l2Norm(VectorCandidate vector, float expected) {
        float norm = VectorUtil.l2Norm(vector);
        assertThat(norm).isCloseTo(expected, CLOSENESS);
    }

    static Stream<Arguments> l2Norm() {
        return Stream.of(
                Arguments.of(origin(42), 0.0f),
                Arguments.of(basisUnitVector(42, 0), 1.0f),
                Arguments.of(vector(1.0f, 0.0f, 0.0f, 0.0f), 1.0f),
                Arguments.of(vector(1.0f, 1.0f, 0.0f, 0.0f), (float) Math.sqrt(2.0)),
                Arguments.of(vector(1.0f, 1.0f, 1.0f, 0.0f), (float) Math.sqrt(3.0)),
                Arguments.of(vector(1.0f, 1.0f, 1.0f, 1.0f), 2.0f),
                Arguments.of(vector(4.0f, 0.0f, 0.0f, 0.0f), 4.0f),
                Arguments.of(vector(4.0f, 4.0f, 0.0f, 0.0f), 4.0f * (float) Math.sqrt(2.0)),
                Arguments.of(vector(4.0f, 4.0f, 4.0f, 0.0f), 4.0f * (float) Math.sqrt(3.0)),
                Arguments.of(vector(4.0f, 4.0f, 4.0f, 4.0f), 8.0f),
                Arguments.of(vector(1.0f, 2.0f, 3.0f, 4.0f), (float) Math.sqrt(30.0)),
                Arguments.of(vector(-1.0f, 2.0f, -3.0f, 4.0f), (float) Math.sqrt(30.0)));
    }

    @ParameterizedTest
    @MethodSource
    void l2NormalizedVector(VectorCandidate vector, VectorCandidate expected) {
        VectorCandidate normalized = VectorUtil.l2NormalizedVector(vector);
        assertThat(normalized.dimensions()).isEqualTo(vector.dimensions()).isEqualTo(expected.dimensions());
        assertThat(normalized).satisfies(n -> {
            for (int i = 0; i < normalized.dimensions(); i++) {
                assertThat(normalized.floatValue(i)).isCloseTo(expected.floatValue(i), CLOSENESS);
            }
        });
    }

    static Stream<Arguments> l2NormalizedVector() {
        return Stream.of(
                Arguments.of(vector(1.0f, 0.0f, 0.0f, 0.0f), vector(1.0f, 0.0f, 0.0f, 0.0f)),
                Arguments.of(
                        vector(1.0f, 1.0f, 0.0f, 0.0f),
                        vector((float) Math.sqrt(2.0) / 2.0f, (float) Math.sqrt(2.0) / 2.0f, 0.0f, 0.0f)),
                Arguments.of(
                        vector(1.0f, 1.0f, 1.0f, 0.0f),
                        vector(
                                (float) Math.sqrt(3.0) / 3.0f,
                                (float) Math.sqrt(3.0) / 3.0f,
                                (float) Math.sqrt(3.0) / 3.0f,
                                0.0f)),
                Arguments.of(vector(1.0f, 1.0f, 1.0f, 1.0f), vector(0.5f, 0.5f, 0.5f, 0.5f)),
                Arguments.of(vector(4.0f, 0.0f, 0.0f, 0.0f), vector(1.0f, 0.0f, 0.0f, 0.0f)),
                Arguments.of(
                        vector(4.0f, 4.0f, 0.0f, 0.0f),
                        vector((float) Math.sqrt(2.0) / 2.0f, (float) Math.sqrt(2.0) / 2.0f, 0.0f, 0.0f)),
                Arguments.of(
                        vector(4.0f, 4.0f, 4.0f, 0.0f),
                        vector(
                                (float) Math.sqrt(3.0) / 3.0f,
                                (float) Math.sqrt(3.0) / 3.0f,
                                (float) Math.sqrt(3.0) / 3.0f,
                                0.0f)),
                Arguments.of(vector(4.0f, 4.0f, 4.0f, 4.0f), vector(0.5f, 0.5f, 0.5f, 0.5f)),
                Arguments.of(
                        vector(1.0f, 2.0f, 3.0f, 4.0f),
                        vector(
                                (float) Math.sqrt(30.0) / 30.0f,
                                (float) Math.sqrt(30.0) / 15.0f,
                                (float) Math.sqrt(30) / 10.0f,
                                (float) Math.sqrt(30.0) * 2.0f / 15.0f)),
                Arguments.of(
                        vector(-1.0f, 2.0f, -3.0f, 4.0f),
                        vector(
                                (float) -Math.sqrt(30.0) / 30.0f,
                                (float) Math.sqrt(30.0) / 15.0f,
                                (float) -Math.sqrt(30) / 10.0f,
                                (float) Math.sqrt(30.0) * 2.0f / 15.0f)));
    }

    @ParameterizedTest
    @MethodSource("unaryFunctions")
    void nullVector(Consumer<VectorCandidate> function) {
        assertThrowsNullVector(() -> function.accept(null));
    }

    @ParameterizedTest
    @MethodSource("binaryFunctions")
    void nullVector(BiConsumer<VectorCandidate, VectorCandidate> function) {
        VectorCandidate vector = randomVector(random.random());
        assertThrowsNullVector(() -> function.accept(vector, null));
        assertThrowsNullVector(() -> function.accept(null, vector));
    }

    static ErrorGqlStatusObjectAssert<?> assertThrowsNullVector(ThrowingCallable callable) {
        return ErrorGqlStatusObjectAssertions.assertThatThrownBy(callable)
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContaining("Vector cannot be null")
                .gqlStatusObject()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N05)
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22004);
    }

    @ParameterizedTest
    @MethodSource("unaryFunctions")
    void nonPositiveDimensions(Consumer<VectorCandidate> function) {
        VectorCandidate dimensionless = vectorCandidate();
        assertThrowsNonPositiveDimensions(() -> function.accept(dimensionless));

        VectorCandidate negative = mock(VectorCandidate.class);
        when(negative.dimensions()).thenReturn(-1);
        assertThrowsNonPositiveDimensions(() -> function.accept(negative));
    }

    @ParameterizedTest
    @MethodSource("binaryFunctions")
    void nonPositiveDimensions(BiConsumer<VectorCandidate, VectorCandidate> function) {
        VectorCandidate dimensionless = vectorCandidate();
        assertThrowsNonPositiveDimensions(() -> function.accept(dimensionless, dimensionless));

        VectorCandidate negative = mock(VectorCandidate.class);
        when(negative.dimensions()).thenReturn(-1);
        assertThrowsNonPositiveDimensions(() -> function.accept(negative, negative));
    }

    static ErrorGqlStatusObjectAssert<?> assertThrowsNonPositiveDimensions(ThrowingCallable callable) {
        return ErrorGqlStatusObjectAssertions.assertThatThrownBy(callable)
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContaining("Vector dimensions requires positive integer argument")
                .gqlStatusObject()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22003)
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N86);
    }

    @ParameterizedTest
    @MethodSource("binaryFunctions")
    void mismatchDimensions(BiConsumer<VectorCandidate, VectorCandidate> function) {
        Pair<VectorCandidate, VectorCandidate> vectors = randomMismatchDimensions();
        assertThrowsMismatchDimensions(() -> function.accept(vectors.getOne(), vectors.getTwo()));
    }

    Pair<VectorCandidate, VectorCandidate> randomMismatchDimensions() {
        int dimensions1 = randomDimensions(random.random());
        int dimensions2;
        do {
            dimensions2 = randomDimensions(random.random());
        } while (dimensions1 == dimensions2);

        return pair(randomVector(random.random(), dimensions1), randomVector(random.random(), dimensions2));
    }

    static ErrorGqlStatusObjectAssert<?> assertThrowsMismatchDimensions(ThrowingCallable callable) {
        return ErrorGqlStatusObjectAssertions.assertThatThrownBy(callable)
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessageContaining("Vector dimensions are required to be the same")
                .gqlStatusObject()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N38)
                .gqlCause()
                .hasGqlStatus(GqlStatusInfoCodes.STATUS_22N04);
    }

    static Stream<Named<Consumer<VectorCandidate>>> unaryFunctions() {
        return Stream.of(
                Named.of("magnitude", VectorUtil::magnitude),
                Named.of("l1Norm", VectorUtil::l1Norm),
                Named.of("l1NormalizedVector", VectorUtil::l1NormalizedVector),
                Named.of("l2Norm", VectorUtil::l2Norm),
                Named.of("l2NormalizedVector", VectorUtil::l2NormalizedVector));
    }

    static Stream<Named<BiConsumer<VectorCandidate, VectorCandidate>>> binaryFunctions() {
        return Stream.of(
                Named.of("dotProduct", VectorUtil::dotProduct),
                Named.of("cosine", VectorUtil::cosine),
                Named.of("l1Distance", VectorUtil::l1Distance),
                Named.of("squareL2Distance", VectorUtil::squareL2Distance),
                Named.of("l2Distance", VectorUtil::l2Distance),
                Named.of("hammingDistance", VectorUtil::hammingDistance));
    }

    @RepeatedTest(5)
    void originCheck() {
        int dimensions = randomDimensions(random.random());
        int base = random.nextInt(0, dimensions);
        assertThat(VectorUtil.origin(origin(dimensions))).isTrue();
        assertThat(VectorUtil.origin(basisUnitVector(dimensions, base))).isFalse();
        // not origin by construction
        assertThat(VectorUtil.origin(randomVector(random.random(), dimensions))).isFalse();
    }

    @RepeatedTest(5)
    void validVectors() {
        int dimensions = randomDimensions(random.random());
        int base = random.nextInt(0, dimensions);
        assertThat(VectorUtil.valid(origin(dimensions))).isTrue();
        assertThat(VectorUtil.valid(basisUnitVector(dimensions, base))).isTrue();
        assertThat(VectorUtil.valid(randomVector(random.random(), dimensions))).isTrue();
    }

    @ParameterizedTest
    @MethodSource
    void invalidVectors(VectorCandidate vector) {
        assertThat(VectorUtil.valid(vector)).isFalse();
    }

    static Stream<Named<VectorCandidate>> invalidVectors() {
        VectorCandidate negative = mock(VectorCandidate.class);
        when(negative.dimensions()).thenReturn(-1);
        return Stream.of(
                Named.of("null", null),
                Named.of("dimensionless", vectorCandidate()),
                Named.of("negative dimensions", negative),
                Named.of("NaN element", vectorCandidate(1.0f, Float.NaN, 3.0f)),
                Named.of("-Inf element", vectorCandidate(1.0f, Float.NEGATIVE_INFINITY, 3.0f)),
                Named.of("+Inf element", vectorCandidate(1.0f, Float.POSITIVE_INFINITY, 3.0f)));
    }
}
