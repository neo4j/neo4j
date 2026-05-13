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

import java.util.Comparator;
import java.util.stream.Stream;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.values.VectorCandidate;

/**
 * Functions which are simply wrappers around {@link VectorUtil} method are tested in
 * {@link VectorUtilTest} and {@link VectorUtilSupportTest} rather than here.
 */
class GQLVectorDistanceFunctionTest {

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = "COSINE")
    void validOrigin(GQLVectorDistanceFunction distanceFunction) {
        assertThat(distanceFunction.valid(origin(42))).isTrue();
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.INCLUDE, names = "COSINE")
    void invalidOrigin(GQLVectorDistanceFunction distanceFunction) {
        assertThat(distanceFunction.valid(origin(42))).isFalse();
    }

    @ParameterizedTest
    @MethodSource
    void euclideanTop10(VectorCandidate query, int... top10Nearest) {
        int[] topKEntityIds = top10EntityIds(query, GQLVectorDistanceFunction.EUCLIDEAN);
        assertThat(topKEntityIds).containsExactly(top10Nearest);
    }

    static Stream<Arguments> euclideanTop10() {
        return Stream.of(
                Arguments.of(vector(1.0f, 0.0f, 0.0f), ids(1, 0, 3, 5, 8, 30, 32, 2, 4, 7)),
                Arguments.of(vector(0.0f, -1.0f, 0.0f), ids(16, 0, 17, 20, 23, 30, 33, 1, 4, 15)),
                Arguments.of(vector(0.0f, 0.0f, 2.0f), ids(11, 4, 5, 6, 31, 33, 7, 35, 36, 37)),
                Arguments.of(vector(2.0f, 0.0f, 2.0f), ids(12, 5, 7, 36, 8, 11, 14, 49, 1, 4)),
                Arguments.of(vector(-1.0f, 1.0f, 0.0f), ids(29, 2, 15, 35, 39, 0, 6, 9, 19, 22)));
    }

    @ParameterizedTest
    @MethodSource
    void euclideanSquaredTop10(VectorCandidate query, int... top10Nearest) {
        int[] topKEntityIds = top10EntityIds(query, GQLVectorDistanceFunction.EUCLIDEAN_SQUARED);
        assertThat(topKEntityIds).containsExactly(top10Nearest);
    }

    static Stream<Arguments> euclideanSquaredTop10() {
        return Stream.of(
                Arguments.of(vector(1.0f, 0.0f, 0.0f), ids(1, 0, 3, 5, 8, 30, 32, 2, 4, 7)),
                Arguments.of(vector(0.0f, -1.0f, 0.0f), ids(16, 0, 17, 20, 23, 30, 33, 1, 4, 15)),
                Arguments.of(vector(0.0f, 0.0f, 2.0f), ids(11, 4, 5, 6, 31, 33, 7, 35, 36, 37)),
                Arguments.of(vector(2.0f, 0.0f, 2.0f), ids(12, 5, 7, 36, 8, 11, 14, 49, 1, 4)),
                Arguments.of(vector(-1.0f, 1.0f, 0.0f), ids(29, 2, 15, 35, 39, 0, 6, 9, 19, 22)));
    }

    @ParameterizedTest
    @MethodSource
    void manhattanTop10(VectorCandidate query, int... top10Nearest) {
        int[] topKEntityIds = top10EntityIds(query, GQLVectorDistanceFunction.MANHATTAN);
        assertThat(topKEntityIds).containsExactly(top10Nearest);
    }

    static Stream<Arguments> manhattanTop10() {
        return Stream.of(
                Arguments.of(vector(1.0f, 0.0f, 0.0f), ids(1, 0, 3, 5, 8, 30, 32, 2, 4, 7)),
                Arguments.of(vector(0.0f, -1.0f, 0.0f), ids(16, 0, 17, 20, 23, 30, 33, 1, 2, 4)),
                Arguments.of(vector(0.0f, 0.0f, 2.0f), ids(11, 4, 0, 5, 6, 12, 13, 31, 33, 44)),
                Arguments.of(vector(2.0f, 0.0f, 2.0f), ids(12, 5, 8, 11, 14, 49, 1, 4, 7, 36)),
                Arguments.of(vector(-1.0f, 1.0f, 0.0f), ids(29, 2, 15, 35, 39, 0, 3, 6, 9, 17)));
    }

    @ParameterizedTest
    @MethodSource
    void cosineTop10(VectorCandidate query, int... top10Nearest) {
        int[] topKEntityIds = top10EntityIds(query, GQLVectorDistanceFunction.COSINE);
        assertThat(topKEntityIds).containsExactly(top10Nearest);
    }

    static Stream<Arguments> cosineTop10() {
        return Stream.of(
                Arguments.of(vector(1.0f, 0.0f, 0.0f), ids(1, 8, 3, 5, 10, 12, 30, 32, 43, 45)),
                Arguments.of(vector(0.0f, -1.0f, 0.0f), ids(16, 23, 17, 20, 24, 27, 30, 33, 43, 46)),
                Arguments.of(vector(0.0f, 0.0f, 2.0f), ids(4, 11, 5, 6, 12, 13, 31, 33, 44, 46)),
                Arguments.of(vector(2.0f, 0.0f, 2.0f), ids(5, 12, 7, 14, 36, 49, 1, 4, 8, 11)),
                Arguments.of(vector(-1.0f, 1.0f, 0.0f), ids(29, 42, 35, 39, 48, 52, 2, 9, 15, 22)));
    }

    @ParameterizedTest
    @MethodSource
    void dotTop10(VectorCandidate query, int... top10Nearest) {
        int[] topKEntityIds = top10EntityIds(query, GQLVectorDistanceFunction.DOT);
        assertThat(topKEntityIds).containsExactly(top10Nearest);
    }

    static Stream<Arguments> dotTop10() {
        return Stream.of(
                Arguments.of(vector(1.0f, 0.0f, 0.0f), ids(8, 10, 12, 14, 43, 45, 49, 51, 53, 1)),
                Arguments.of(vector(0.0f, -1.0f, 0.0f), ids(23, 24, 27, 28, 43, 46, 49, 50, 53, 54)),
                Arguments.of(vector(0.0f, 0.0f, 2.0f), ids(11, 12, 13, 14, 44, 46, 48, 49, 50, 4)),
                Arguments.of(vector(2.0f, 0.0f, 2.0f), ids(12, 14, 49, 5, 7, 8, 10, 11, 13, 36)),
                Arguments.of(vector(-1.0f, 1.0f, 0.0f), ids(42, 48, 52, 9, 13, 22, 26, 29, 35, 39)));
    }

    @ParameterizedTest
    @MethodSource
    void hammingTop10(VectorCandidate query, int... top10Nearest) {
        int[] topKEntityIds = top10EntityIds(query, GQLVectorDistanceFunction.HAMMING);
        assertThat(topKEntityIds).containsExactly(top10Nearest);
    }

    static Stream<Arguments> hammingTop10() {
        return Stream.of(
                Arguments.of(vector(1.0f, 0.0f, 0.0f), ids(1, 0, 3, 5, 8, 15, 22, 30, 32, 2)),
                Arguments.of(vector(0.0f, -1.0f, 0.0f), ids(16, 0, 2, 9, 17, 20, 23, 30, 33, 1)),
                Arguments.of(vector(0.0f, 0.0f, 2.0f), ids(11, 0, 4, 12, 13, 18, 25, 44, 46, 1)),
                Arguments.of(vector(2.0f, 0.0f, 2.0f), ids(12, 8, 11, 14, 44, 45, 49, 0, 1, 4)),
                Arguments.of(vector(-1.0f, 1.0f, 0.0f), ids(29, 2, 3, 15, 17, 35, 39, 0, 1, 6)));
    }

    private static int[] ids(int... ids) {
        return ids;
    }

    private static int[] top10EntityIds(VectorCandidate query, VectorDistanceFunction distanceFunction) {
        return ENTITIES.asLazy()
                .select(entity -> distanceFunction.valid(entity.vector))
                .collect(entity -> new Result(entity, distanceFunction.distance(entity.vector, query)))
                .toSortedList()
                .take(10)
                .collectInt(result -> result.entity.id)
                .toArray();
    }

    private record Entity(int id, VectorCandidate vector) {}

    private record Result(Entity entity, float distance) implements Comparable<Result> {
        private static final Comparator<Result> COMPARATOR =
                Comparator.comparingDouble(Result::distance).thenComparingInt(result -> result.entity.id);

        @Override
        public int compareTo(Result result) {
            return COMPARATOR.compare(this, result);
        }
    }

    private static final RichIterable<Entity> ENTITIES = Lists.fixedSize.of(
            new Entity(0, vector(0.0f, 0.0f, 0.0f)),
            new Entity(1, vector(1.0f, 0.0f, 0.0f)),
            new Entity(2, vector(0.0f, 1.0f, 0.0f)),
            new Entity(3, vector(1.0f, 1.0f, 0.0f)),
            new Entity(4, vector(0.0f, 0.0f, 1.0f)),
            new Entity(5, vector(1.0f, 0.0f, 1.0f)),
            new Entity(6, vector(0.0f, 1.0f, 1.0f)),
            new Entity(7, vector(1.0f, 1.0f, 1.0f)),
            new Entity(8, vector(2.0f, 0.0f, 0.0f)),
            new Entity(9, vector(0.0f, 2.0f, 0.0f)),
            new Entity(10, vector(2.0f, 2.0f, 0.0f)),
            new Entity(11, vector(0.0f, 0.0f, 2.0f)),
            new Entity(12, vector(2.0f, 0.0f, 2.0f)),
            new Entity(13, vector(0.0f, 2.0f, 2.0f)),
            new Entity(14, vector(2.0f, 2.0f, 2.0f)),
            new Entity(15, vector(-1.0f, 0.0f, 0.0f)),
            new Entity(16, vector(0.0f, -1.0f, 0.0f)),
            new Entity(17, vector(-1.0f, -1.0f, 0.0f)),
            new Entity(18, vector(0.0f, 0.0f, -1.0f)),
            new Entity(19, vector(-1.0f, 0.0f, -1.0f)),
            new Entity(20, vector(0.0f, -1.0f, -1.0f)),
            new Entity(21, vector(-1.0f, -1.0f, -1.0f)),
            new Entity(22, vector(-2.0f, 0.0f, 0.0f)),
            new Entity(23, vector(0.0f, -2.0f, 0.0f)),
            new Entity(24, vector(-2.0f, -2.0f, 0.0f)),
            new Entity(25, vector(0.0f, 0.0f, -2.0f)),
            new Entity(26, vector(-2.0f, 0.0f, -2.0f)),
            new Entity(27, vector(0.0f, -2.0f, -2.0f)),
            new Entity(28, vector(-2.0f, -2.0f, -2.0f)),
            new Entity(29, vector(-1.0f, 1.0f, 0.0f)),
            new Entity(30, vector(1.0f, -1.0f, 0.0f)),
            new Entity(31, vector(-1.0f, 0.0f, 1.0f)),
            new Entity(32, vector(1.0f, 0.0f, -1.0f)),
            new Entity(33, vector(0.0f, -1.0f, 1.0f)),
            new Entity(34, vector(0.0f, 1.0f, -1.0f)),
            new Entity(35, vector(-1.0f, 1.0f, 1.0f)),
            new Entity(36, vector(1.0f, -1.0f, 1.0f)),
            new Entity(37, vector(-1.0f, -1.0f, 1.0f)),
            new Entity(38, vector(1.0f, 1.0f, -1.0f)),
            new Entity(39, vector(-1.0f, 1.0f, -1.0f)),
            new Entity(40, vector(1.0f, -1.0f, -1.0f)),
            new Entity(41, vector(-1.0f, -1.0f, -1.0f)),
            new Entity(42, vector(-2.0f, 2.0f, 0.0f)),
            new Entity(43, vector(2.0f, -2.0f, 0.0f)),
            new Entity(44, vector(-2.0f, 0.0f, 2.0f)),
            new Entity(45, vector(2.0f, 0.0f, -2.0f)),
            new Entity(46, vector(0.0f, -2.0f, 2.0f)),
            new Entity(47, vector(0.0f, 2.0f, -2.0f)),
            new Entity(48, vector(-2.0f, 2.0f, 2.0f)),
            new Entity(49, vector(2.0f, -2.0f, 2.0f)),
            new Entity(50, vector(-2.0f, -2.0f, 2.0f)),
            new Entity(51, vector(2.0f, 2.0f, -2.0f)),
            new Entity(52, vector(-2.0f, 2.0f, -2.0f)),
            new Entity(53, vector(2.0f, -2.0f, -2.0f)),
            new Entity(54, vector(-2.0f, -2.0f, -2.0f)));
}
