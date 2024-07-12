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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.genai.util.ThrowingSupplierAssert.assertThatThrownBy;
import static org.neo4j.genai.vector.VectorEncoding.BatchRow;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.genai.vector.MalformedGenAIResponseException;

@TestInstance(Lifecycle.PER_CLASS)
abstract class ParsingTestBase {

    private final ResponseParser responseParser;

    protected ParsingTestBase(ResponseParser responseParser) {
        this.responseParser = responseParser;
    }

    abstract Collection<Arguments> parseResponse();

    @ParameterizedTest
    @MethodSource
    void parseResponse(ExpectedOutcome outcome, String json) throws MalformedGenAIResponseException {
        final var responseStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        outcome.assertExpected(() -> responseParser
                .parseResponse(outcome.resources(), responseStream, EMPTY_INT_ARRAY)
                .toList());
    }

    interface ResponseParser {
        Stream<BatchRow> parseResponse(List<String> resources, InputStream responseStream, int[] nullIndexes)
                throws MalformedGenAIResponseException;
    }

    interface ExpectedOutcome {
        void assertExpected(ThrowingSupplier<List<BatchRow>, MalformedGenAIResponseException> code)
                throws MalformedGenAIResponseException;

        List<String> resources();
    }

    record ExpectedVectors(List<String> resources, List<float[]> expected) implements ExpectedOutcome {
        @Override
        public void assertExpected(ThrowingSupplier<List<BatchRow>, MalformedGenAIResponseException> code)
                throws MalformedGenAIResponseException {
            assertThat(resources).hasSameSizeAs(expected);
            final var indices = IntStream.range(0, resources.size()).boxed().toList();
            assertThat(code.get()).zipSatisfy(indices, (batchRow, index) -> {
                final var resource = resources().get(index);
                final var expected = expected().get(index);
                assertThat(batchRow.index()).isEqualTo((long) index);
                assertThat(batchRow.resource()).isEqualTo(resource);
                assertThat(batchRow.vector()).isEqualTo(expected);
            });
        }
    }

    record ExpectedError(List<String> resources, String... messageFragments) implements ExpectedOutcome {
        @Override
        public void assertExpected(ThrowingSupplier<List<BatchRow>, MalformedGenAIResponseException> code) {
            assertThatThrownBy(code)
                    .findFirstInstanceOfInCauseChain(MalformedGenAIResponseException.class)
                    .hasMessageContainingAll(messageFragments);
        }

        @Override
        public String toString() {
            return "ExpectedError" + Arrays.toString(messageFragments);
        }
    }
}
