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
package org.neo4j.genai.vector.providers;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.genai.util.ParametersTest;
import org.neo4j.genai.vector.VectorEncoding;
import org.neo4j.genai.vector.VectorEncoding.InternalBatchRow;
import org.neo4j.kernel.api.impl.schema.vector.VectorSimilarityFunctions;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@TestInstance(Lifecycle.PER_CLASS)
abstract class BaseIT {
    private static final VectorEncoding VECTOR_ENCODING = new VectorEncoding();
    private static final List<String> RESOURCES = Arrays.asList(
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
            null,
            "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
            null,
            "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");

    private final String provider;
    private final Map<String, ?> config;
    private final List<float[]> expectedVectors;

    static List<float[]> loadExpectedEmbeddings(String filename) {
        final var vectors = new ArrayList<float[]>();
        final var scanner = new Scanner(requireNonNull(BaseIT.class.getResourceAsStream(filename)));
        while (scanner.hasNextLine()) {
            final String line = scanner.nextLine();
            if (line.equals("null")) {
                vectors.add(null);
            } else {
                final var vector = ArrayUtils.toPrimitive(
                        Arrays.stream(line.split(", ")).map(Float::parseFloat).toArray(Float[]::new));
                vectors.add(vector);
            }
        }
        return vectors;
    }

    protected BaseIT(String provider, Map<String, ?> config, List<float[]> expectedVectors) {
        this.provider = provider;
        this.config = config;
        this.expectedVectors = expectedVectors;
    }

    protected BaseIT(
            String provider,
            String expectedVectorsFileName,
            Map<String, ?> baseConfig,
            Map<String, ?> configExtension) {
        this.provider = provider;
        final var config = new HashMap<String, Object>();
        config.putAll(baseConfig);
        config.putAll(configExtension);
        this.config = config;
        this.expectedVectors = loadExpectedEmbeddings(expectedVectorsFileName);
    }

    @ParameterizedTest
    @MethodSource
    void shouldGenerateApproximatelyExpectedEmbeddings(Supplier<Stream<Value>> supplier) {
        final var similarity = VectorSimilarityFunctions.EUCLIDEAN;
        assertThat(supplier.get()).zipSatisfy(expectedVectors, (vector, expectedVector) -> {
            if (expectedVector == null) {
                assertThat(vector).isEqualTo(Values.NO_VALUE);
            } else {
                final var score = similarity.compare(similarity.maybeToValidVector(vector), expectedVector);
                assertThat(score).as("should be similar").isCloseTo(1.f, withinPercentage(1));
            }
        });
    }

    Stream<Named<Supplier<Stream<Value>>>> shouldGenerateApproximatelyExpectedEmbeddings() {
        return Stream.of(
                Named.of("single", () -> RESOURCES.stream()
                        .map(resource -> VECTOR_ENCODING.encode(resource, provider, ParametersTest.from(config)))),
                Named.of("batched", () -> VECTOR_ENCODING
                        .encode(RESOURCES, provider, ParametersTest.from(config))
                        .map(InternalBatchRow::vector)));
    }
}
