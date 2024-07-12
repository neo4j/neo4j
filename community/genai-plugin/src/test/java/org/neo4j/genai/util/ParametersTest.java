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
package org.neo4j.genai.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.values.storable.Values.NO_VALUE;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Maps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.genai.util.Parameters.Parameter;
import org.neo4j.genai.util.Parameters.ParameterType;
import org.neo4j.genai.util.Parameters.Required;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

public class ParametersTest {

    private static final MapValue EMPTY_CONFIG = from(Map.of());

    static class MyParams {
        String model;
        Optional<String> taskType;
        Optional<Long> dimensions;
    }

    static class DefaultParams {
        String model = "myModel";
        long dimensions = 1024;
        Long vibes = 123L;
        Optional<String> thing;
    }

    @Test
    void shouldMapToCypherTypeNames() {
        assertThat(Parameters.getParameters(MyParams.class))
                .map(Parameter::type)
                .map(ParameterType::cypherName)
                .containsExactly("STRING NOT NULL", "STRING", "INTEGER");
    }

    @Test
    void shouldInferNullable() {
        assertThat(Parameters.getParameters(MyParams.class))
                .map(Parameter::type)
                .map(ParameterType::nullable)
                .containsExactly(false, true, true);

        assertThat(Parameters.getParameters(DefaultParams.class))
                .map(Parameter::type)
                .map(ParameterType::nullable)
                .containsExactly(false, false, false, true);
    }

    @Test
    void shouldInferRequired() {
        assertThat(Parameters.getParameters(MyParams.class))
                .map(Parameter::isRequired)
                .containsExactly(true, false, false);

        assertThat(Parameters.getParameters(DefaultParams.class))
                .map(Parameter::isRequired)
                .containsExactly(false, false, false, false);
    }

    @Test
    void shouldParseFull() {
        final var model = "myModel";
        final var taskType = "someTask";
        final var dimensions = 1337L;

        final var params = Parameters.parse(
                MyParams.class,
                from(Map.of(
                        "model", model,
                        "taskType", taskType,
                        "dimensions", dimensions)));

        assertThat(params.model).isEqualTo(model);
        assertThat(params.taskType).hasValue(taskType);
        assertThat(params.dimensions).hasValue(dimensions);
    }

    @Test
    void shouldParsePartial() {
        final var model = "myModel";
        final var taskType = "someTask";

        final var params = Parameters.parse(
                MyParams.class,
                from(Map.of(
                        "model", model,
                        "taskType", taskType)));

        assertThat(params.model).isEqualTo(model);
        assertThat(params.taskType).hasValue(taskType);
        assertThat(params.dimensions).isEmpty();
    }

    @Test
    void shouldThrowOnWrongRequiredType() {
        final var model = 123;
        final var map = Map.of("model", model);

        assertThatThrownBy(() -> Parameters.parse(MyParams.class, from(map)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'model' is expected to have been of type STRING NOT NULL");
    }

    @Test
    void shouldThrowOnMissingRequired() {
        assertThatThrownBy(() -> Parameters.parse(MyParams.class, EMPTY_CONFIG))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is expected to have been set");
    }

    @Test
    void shouldThrowOnAssigningNullToNonNullable() {
        final var map = from(Maps.mutable.of("model", null));

        assertThatThrownBy(() -> Parameters.parse(MyParams.class, map))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'model' is expected to be non-null");
    }

    @Test
    void shouldSucceedOnAssigningNulltoNullable() {
        final var map = from(Maps.mutable.of("model", "model", "dimensions", null));

        final var params = Parameters.parse(MyParams.class, map);
        assertThat(params.dimensions).isEmpty();
    }

    @Test
    void shouldThrowOnWrongOptionalType() {
        final var model = "myModel";
        final var taskType = 123;

        final var map = Map.of(
                "model", model,
                "taskType", taskType);

        assertThatThrownBy(() -> Parameters.parse(MyParams.class, from(map)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'taskType' is expected to have been of type STRING");
    }

    @Test
    void shouldDefaultValues() {
        final var params = Parameters.parse(DefaultParams.class, EMPTY_CONFIG);

        assertThat(params.model).isEqualTo("myModel");
        assertThat(params.dimensions).isEqualTo(1024);
        assertThat(params.vibes).isEqualTo(123);
        assertThat(params.thing).isEmpty();
    }

    @Test
    void shouldOverrideDefaultValues() {
        final var params = Parameters.parse(DefaultParams.class, from(Map.of("dimensions", 1337, "thing", "goodbye")));

        assertThat(params.model).isEqualTo("myModel");
        assertThat(params.dimensions).isEqualTo(1337);
        assertThat(params.vibes).isEqualTo(123);
        assertThat(params.thing).hasValue("goodbye");
    }

    static class OptionalWithDefault {
        OptionalDouble value = OptionalDouble.of(Math.PI);
    }

    @Test
    void shouldThrowWithDefaultedOptional() {
        assertThatThrownBy(() -> Parameters.getParameters(OptionalWithDefault.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'value' cannot have a default value and have type 'OptionalDouble'");
    }

    static class PrimitiveLongParams {
        long value;
    }

    static class BoxedLongParams {
        Long value;
    }

    static class PrimitiveDoubleParams {
        double value;
    }

    static class BoxedDoubleParams {
        Double value;
    }

    static class PrimitiveBoolParams {
        boolean value;
    }

    static class BoxedBoolParams {
        Boolean value;
    }

    static class StringParams {
        String value;
    }

    @Test
    void shouldParsePrimitives() {
        assertParameterParsed(PrimitiveLongParams.class, (byte) 123, 123L);
        assertParameterParsed(PrimitiveLongParams.class, (short) 123, 123L);
        assertParameterParsed(PrimitiveLongParams.class, 123, 123L);
        assertParameterParsed(PrimitiveLongParams.class, 123L, 123L);
        assertParameterParsed(BoxedLongParams.class, (byte) 123, 123L);
        assertParameterParsed(BoxedLongParams.class, (short) 123, 123L);
        assertParameterParsed(BoxedLongParams.class, 123, 123L);
        assertParameterParsed(BoxedLongParams.class, 123L, 123L);

        assertParameterParsed(PrimitiveDoubleParams.class, 123.0f, 123.0d);
        assertParameterParsed(PrimitiveDoubleParams.class, 123.0d, 123.0d);
        assertParameterParsed(BoxedDoubleParams.class, 123.0f, 123.0d);
        assertParameterParsed(BoxedDoubleParams.class, 123.0d, 123.0d);

        assertParameterParsed(PrimitiveBoolParams.class, true, true);
        assertParameterParsed(BoxedBoolParams.class, true, true);

        assertParameterParsed(StringParams.class, 'x', "x");
    }

    private <T> void assertParameterParsed(Class<T> cls, Object value, Object expected) {
        final String fieldName = "value";
        try {
            final Field field = cls.getDeclaredField(fieldName);
            field.setAccessible(true);
            var params = Parameters.parse(cls, from(Map.of(fieldName, value)));
            assertThat(field.get(params)).isEqualTo(expected);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class MyClassParams {
        public String model;
        public String taskType = "defaultTaskType";
        public Optional<Long> dimensions;

        long defaultedLong = 0;
    }

    @Test
    void shouldParseClassParams() {
        final var map = from(Map.of(
                "model", "noah",
                "dimensions", 1234,
                "defaultedLong", 8));

        final var params = Parameters.parse(MyClassParams.class, map);
        assertThat(params.dimensions).hasValue(1234L);
    }

    static class RequiredPrimitive {
        @Required
        long requiredLong;
    }

    @Test
    void shouldParseRequiredPrimitive() {
        assertThatThrownBy(() -> Parameters.parse(RequiredPrimitive.class, EMPTY_CONFIG))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'requiredLong' is expected to have been set");
    }

    static class RequiredNonPrimitiveWithDefault {
        @Required
        String requiredString = "foo";
    }

    @Test
    void shouldThrowWhenRequiredNonPrimitiveWithDefault() {
        assertThatThrownBy(() -> Parameters.getParameters(RequiredNonPrimitiveWithDefault.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'requiredString' cannot have a default and be explicitly annotated '@Required'");
    }

    static class RequiredOptional {
        @Required
        OptionalLong requiredLong;
    }

    @Test
    void shouldThrowWhenRequiredOptionalType() {
        assertThatThrownBy(() -> Parameters.getParameters(RequiredOptional.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "'requiredLong' cannot be explicitly annotated '@Required' and have type 'OptionalLong'");
    }

    static class DynamicParameters extends Parameters.WithDynamic {
        String model;
    }

    @Test
    void dynamicValuesShouldNotIncludeDeclaredParameters() {
        final var map = from(Map.of("model", "hello"));
        final var params = Parameters.parse(DynamicParameters.class, map);
        assertThat(params.dynamic()).isEmpty();
    }

    @Test
    void dynamicValuesShouldIncludeRemainingValues() {
        final var map = from(Map.of("model", "hello", "dimensions", 1337));
        final var params = Parameters.parse(DynamicParameters.class, map);
        assertThat(params.dynamic()).containsEntry("dimensions", 1337L);
    }

    private static class PrivateParams {}

    @Test
    void shouldThrowInformativeErrorOnMissingConstructor() {
        assertThatThrownBy(() -> Parameters.parse(PrivateParams.class, EMPTY_CONFIG))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PrivateParams must have an accessible zero-argument constructor");
    }

    static class PrivateField {
        private String message;
        String model;
    }

    @Test
    void shouldIgnoreInaccessibleFields() {
        final var parameters = Parameters.getParameters(PrivateField.class);
        assertThat(parameters).satisfiesExactlyInAnyOrder(parameter -> assertThat(parameter.name())
                .isEqualTo("model"));
    }

    static class UnmappableType {
        int model;
    }

    @Test
    void shouldThrowInformativeErrorOnUnmappableType() {
        assertThatThrownBy(() -> Parameters.getParameters(UnmappableType.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        "Parameter 'model' is of an unsupported type",
                        "Don't know how to map `int` to the Neo4j Type System.");
    }

    static class PrimitiveOptionals {
        OptionalLong longValue;
        OptionalDouble doubleValue;
    }

    @Test
    void shouldSupportPrimitiveOptionals() {
        final var params =
                Parameters.parse(PrimitiveOptionals.class, from(Map.of("longValue", 123L, "doubleValue", 3.0f)));

        assertThat(params.longValue).hasValue(123L);
        assertThat(params.doubleValue).hasValue(3.0);
    }

    @Test
    void shouldSupportSettingPrimitiveOptionalToNull() {
        final var params = Parameters.parse(
                PrimitiveOptionals.class, from(Map.of("longValue", NO_VALUE, "doubleValue", NO_VALUE)));

        assertThat(params.longValue).isEmpty();
        assertThat(params.doubleValue).isEmpty();
    }

    @Test
    void shouldSupportDefaultedPrimitiveOptionals() {
        final var params = Parameters.parse(PrimitiveOptionals.class, EMPTY_CONFIG);

        assertThat(params.longValue).isEmpty();
        assertThat(params.doubleValue).isEmpty();
    }

    static class ProductTypes {
        List<Long> longList = List.of();
        Map<String, String> stringToStringMap = Map.of();
        Map<String, List<Double>> stringToDoubleList = Map.of();
    }

    @ParameterizedTest
    @MethodSource
    void shouldSupportProductTypes(String key, Object value, Consumer<ProductTypes> assertion) {
        final var mapValue = from(Map.of(key, value));
        final var parameters = Parameters.parse(ProductTypes.class, mapValue);
        assertion.accept(parameters);
    }

    private static Stream<Arguments> shouldSupportProductTypes() {
        return Stream.of(
                Arguments.of("longList", new long[] {1, 2, 3}, (Consumer<ProductTypes>)
                        params -> assertThat(params.longList).containsExactly(1L, 2L, 3L)),
                Arguments.of(
                        "longList",
                        VirtualValues.list(Values.longValue(1L), Values.longValue(2L), Values.longValue(3L)),
                        (Consumer<ProductTypes>)
                                params -> assertThat(params.longList).containsExactly(1L, 2L, 3L)),
                Arguments.of("stringToStringMap", from(Map.of("foo", "bar", "baz", "qux")), (Consumer<ProductTypes>)
                        params -> assertThat(params.stringToStringMap)
                                .containsExactlyInAnyOrderEntriesOf(Map.of("foo", "bar", "baz", "qux"))),
                Arguments.of(
                        "stringToDoubleList",
                        from(Map.of("irrational", new double[] {Math.PI, Math.E}, "rational", new double[] {1.0, 0.0})),
                        (Consumer<ProductTypes>) params -> assertThat(params.stringToDoubleList)
                                .containsExactlyInAnyOrderEntriesOf(Map.of(
                                        "irrational", List.of(Math.PI, Math.E), "rational", List.of(1.0, 0.0)))));
    }

    public static MapValue from(Map<String, ?> map) {
        assertThat(map).isNotNull();

        final var mapOfValues = Maps.mutable.<String, AnyValue>empty();
        for (var entry : map.entrySet()) {
            final var key = entry.getKey();
            final var value = entry.getValue();
            if (value instanceof AnyValue anyValue) {
                mapOfValues.put(key, anyValue);
            } else {
                mapOfValues.put(key, Values.of(value));
            }
        }

        return VirtualValues.fromMap(mapOfValues, 1, 1);
    }
}
