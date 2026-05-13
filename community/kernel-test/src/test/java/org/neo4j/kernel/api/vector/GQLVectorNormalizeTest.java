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
import static org.neo4j.kernel.api.vector.VectorUtilSupportTest.origin;
import static org.neo4j.kernel.api.vector.VectorUtilSupportTest.vector;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.values.VectorCandidate;

/**
 * Functions which are simply wrappers around {@link VectorUtil} method are tested in
 * {@link VectorUtilTest} rather than here.
 */
public class GQLVectorNormalizeTest {

    @ParameterizedTest
    @MethodSource
    void nonFiniteNormInvalid(VectorNormalize normalization, VectorCandidate vector, boolean expectedValidity) {
        boolean valid;
        try {
            valid = normalization.valid(vector);
        } catch (AssertionError e) {
            // asserts are usually disabled in production, though not in testing;
            // if the assert triggers it would be invalid
            valid = false;
        }
        assertThat(valid).isEqualTo(expectedValidity);
    }

    static Stream<Arguments> nonFiniteNormInvalid() {
        final float manhattanMax = Float.MAX_VALUE / 4;
        float overManhattanMax = Math.nextUp(manhattanMax);
        float underManhattanMax = Math.nextDown(manhattanMax);
        VectorCandidate manhattanMaxVector = vector(manhattanMax, manhattanMax, manhattanMax, manhattanMax);
        VectorCandidate overManhattanMaxVector =
                vector(overManhattanMax, overManhattanMax, overManhattanMax, overManhattanMax);
        VectorCandidate underManhattanMaxVector =
                vector(underManhattanMax, underManhattanMax, underManhattanMax, underManhattanMax);

        float euclideanMax = (float) Math.sqrt(manhattanMax);
        float overEuclideanMax = Math.nextUp(euclideanMax);
        float underEuclideanMax = Math.nextDown(euclideanMax);
        VectorCandidate euclideanMaxVector = vector(euclideanMax, euclideanMax, euclideanMax, euclideanMax);
        VectorCandidate overEuclideanMaxVector =
                vector(overEuclideanMax, overEuclideanMax, overEuclideanMax, overEuclideanMax);
        VectorCandidate underEuclideanMaxVector =
                vector(underEuclideanMax, underEuclideanMax, underEuclideanMax, underEuclideanMax);

        return Stream.of(
                Arguments.of(GQLVectorNormalize.EUCLIDEAN, underManhattanMaxVector, false),
                Arguments.of(GQLVectorNormalize.EUCLIDEAN, manhattanMaxVector, false),
                Arguments.of(GQLVectorNormalize.EUCLIDEAN, overManhattanMaxVector, false),
                Arguments.of(GQLVectorNormalize.EUCLIDEAN, underEuclideanMaxVector, true),
                Arguments.of(GQLVectorNormalize.EUCLIDEAN, euclideanMaxVector, true),
                Arguments.of(GQLVectorNormalize.EUCLIDEAN, overEuclideanMaxVector, false),
                Arguments.of(GQLVectorNormalize.MANHATTAN, underManhattanMaxVector, true),
                Arguments.of(GQLVectorNormalize.MANHATTAN, manhattanMaxVector, true),
                Arguments.of(GQLVectorNormalize.MANHATTAN, overManhattanMaxVector, false),
                Arguments.of(GQLVectorNormalize.MANHATTAN, underEuclideanMaxVector, true),
                Arguments.of(GQLVectorNormalize.MANHATTAN, euclideanMaxVector, true),
                Arguments.of(GQLVectorNormalize.MANHATTAN, overEuclideanMaxVector, true));
    }

    @ParameterizedTest
    @EnumSource
    void originInvalid(GQLVectorNormalize normalization) {
        assertThat(normalization.valid(origin(42))).isFalse();
    }
}
