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
package org.neo4j.genai.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.genai.vector.providers.TestProvider;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(SuppressOutputExtension.class)
@ResourceLock(Resources.SYSTEM_OUT)
abstract class VectorEncodingArgumentBase {
    protected final EncodeRunnable<String, Value> single = this::single;

    protected final EncodeRunnable<List<String>, List<Value>> batch = this::batch;

    abstract Value single(String resource, String provider, Map<String, ?> configuration);

    abstract List<Value> batch(List<String> resources, String provider, Map<String, ?> configuration);

    @ParameterizedTest
    @MethodSource
    void shouldThrowOnUnsupportedProvider(Consumer<String> consumer) {
        final var unsupported = "unsupported";
        assertThatThrownBy(() -> consumer.accept(unsupported))
                .hasMessageContainingAll("Vector encoding provider not supported", unsupported);
    }

    Stream<Named<Consumer<String>>> shouldThrowOnUnsupportedProvider() {
        return Stream.of(
                Named.of("single", provider -> single.encode("something", provider)),
                Named.of("batch", provider -> batch.encode(List.of("something", "other"), provider)));
    }

    @ParameterizedTest
    @MethodSource
    void shouldThrowOnNullArgumentToEncode(NullArgument<float[]> argument) {
        assertThatThrownBy(argument.supplier::get).hasMessageContainingAll(argument.parameter(), "must not be null");
    }

    private Stream<NullArgument<Value>> shouldThrowOnNullArgumentToEncode() {
        return Stream.of(
                new NullArgument<>("provider", () -> single.encode("something", null)),
                new NullArgument<>("configuration", () -> single.encode("something", TestProvider.NAME, null)));
    }

    @Test
    void shouldNotThrowOnNullResourceToEncode() {
        assertThatCode(() -> single.encode(null, TestProvider.NAME)).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @MethodSource
    void shouldThrowOnNullArgumentToEncodeBatch(NullArgument<List<float[]>> argument) {
        assertThatThrownBy(argument.supplier::get).hasMessageContainingAll(argument.parameter(), "must not be null");
    }

    @Test
    void shouldEncodeVector() {
        final var vector = single.encode("something", TestProvider.NAME);
        assertThat(vector).isEqualTo(TestProvider.VECTOR);
    }

    @Test
    void shouldEncodeNull() {
        final var vector = single.encode(null, TestProvider.NAME);
        assertThat(vector).isEqualTo(Values.NO_VALUE);
    }

    private Stream<NullArgument<List<Value>>> shouldThrowOnNullArgumentToEncodeBatch() {
        return Stream.of(
                new NullArgument<>("resources", () -> batch.encode(null, TestProvider.NAME)),
                new NullArgument<>("provider", () -> batch.encode(List.of("something", "other"), null)),
                new NullArgument<>(
                        "configuration", () -> batch.encode(List.of("something", "other"), TestProvider.NAME, null)));
    }

    @Test
    void shouldThrowOnEmptyResources() {
        assertThatThrownBy(() -> batch.encode(List.of(), TestProvider.NAME))
                .hasMessageContainingAll("resources", "must not be empty");
    }

    @Test
    void shouldNotThrowOnNullResourceInBatch() {
        assertThatCode(() -> batch.encode(Lists.mutable.of((String) null), TestProvider.NAME))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldEncodeBatchedVectors() {
        final var vectors = batch.encode(List.of("something", "other"), TestProvider.NAME);
        assertThat(vectors).containsExactly(TestProvider.VECTOR, TestProvider.VECTOR);
    }

    @Test
    void shouldEncodeBatchedVectorsAndNulls() {
        final var input = Arrays.asList("something", "other", null, "one_more", null);
        final var vectors = batch.encode(input, TestProvider.NAME);
        assertThat(vectors)
                .containsExactly(
                        TestProvider.VECTOR,
                        TestProvider.VECTOR,
                        Values.NO_VALUE,
                        TestProvider.VECTOR,
                        Values.NO_VALUE);
    }

    @Test
    void shouldEncodeBatchedOnlyNulls() {
        final List<String> input = Arrays.asList(null, null);
        final var vectors = batch.encode(input, TestProvider.NAME);
        assertThat(vectors).containsExactly(Values.NO_VALUE, Values.NO_VALUE);
    }

    protected interface EncodeRunnable<T, V> {
        V encode(T resource, String provider, Map<String, ?> configuration);

        default V encode(T resource, String provider) {
            return encode(resource, provider, Map.of("model", "test"));
        }
    }

    protected record NullArgument<V>(String parameter, Supplier<V> supplier) {
        @Override
        public String toString() {
            return null + " " + parameter;
        }
    }
}
