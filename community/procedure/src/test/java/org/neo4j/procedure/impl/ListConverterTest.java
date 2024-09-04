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
package org.neo4j.procedure.impl;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.cypher.internal.evaluator.Evaluator.expressionEvaluator;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTFloat;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;

class ListConverterTest {

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleNullString(CypherVersion version) {
        // Given
        ListConverter converter = new ListConverter(String.class, NTString, expressionEvaluator(version));
        String listString = "null";

        // When
        DefaultParameterValue converted = converter.apply(listString);

        // Then
        assertThat(converted).isEqualTo(ntList(null, NTString));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleEmptyList(CypherVersion version) {
        // Given
        ListConverter converter = new ListConverter(String.class, NTString, expressionEvaluator(version));
        String listString = "[]";

        // When
        DefaultParameterValue converted = converter.apply(listString);

        // Then
        assertThat(converted).isEqualTo(ntList(emptyList(), NTString));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleEmptyListWithSpaces(CypherVersion version) {
        // Given
        ListConverter converter = new ListConverter(String.class, NTString, expressionEvaluator(version));
        String listString = " [  ]   ";

        // When
        DefaultParameterValue converted = converter.apply(listString);

        // Then
        assertThat(converted).isEqualTo(ntList(emptyList(), NTString));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleSingleQuotedValue(CypherVersion version) {
        // Given
        ListConverter converter = new ListConverter(String.class, NTString, expressionEvaluator(version));
        String listString = "['foo', 'bar']";

        // When
        DefaultParameterValue converted = converter.apply(listString);

        // Then
        assertThat(converted).isEqualTo(ntList(asList("foo", "bar"), NTString));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleDoubleQuotedValue(CypherVersion version) {
        // Given
        ListConverter converter = new ListConverter(String.class, NTString, expressionEvaluator(version));
        String listString = "[\"foo\", \"bar\"]";

        // When
        DefaultParameterValue converted = converter.apply(listString);

        // Then
        assertThat(converted).isEqualTo(ntList(asList("foo", "bar"), NTString));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleIntegerValue(CypherVersion version) {
        // Given
        ListConverter converter = new ListConverter(Long.class, NTInteger, expressionEvaluator(version));
        String listString = "[1337, 42]";

        // When
        DefaultParameterValue converted = converter.apply(listString);

        // Then
        assertThat(converted).isEqualTo(ntList(asList(1337L, 42L), NTInteger));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleFloatValue(CypherVersion version) {
        // Given
        ListConverter converter = new ListConverter(Double.class, NTFloat, expressionEvaluator(version));
        String listSting = "[2.718281828, 3.14]";

        // When
        DefaultParameterValue converted = converter.apply(listSting);

        // Then
        assertThat(converted).isEqualTo(ntList(asList(2.718281828, 3.14), NTFloat));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleNullValue(CypherVersion version) {
        // Given
        ListConverter converter = new ListConverter(Double.class, NTFloat, expressionEvaluator(version));
        String listString = "[null]";

        // When
        DefaultParameterValue converted = converter.apply(listString);

        // Then
        assertThat(converted).isEqualTo(ntList(singletonList(null), NTFloat));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleBooleanValues(CypherVersion version) {
        // Given
        ListConverter converter = new ListConverter(Boolean.class, NTBoolean, expressionEvaluator(version));
        String mapString = "[false, true]";

        // When
        DefaultParameterValue converted = converter.apply(mapString);

        // Then
        assertThat(converted).isEqualTo(ntList(asList(false, true), NTBoolean));
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleNestedLists(CypherVersion version) {
        // Given
        ParameterizedType type = mock(ParameterizedType.class);
        when(type.getActualTypeArguments()).thenReturn(new Type[] {Object.class});
        ListConverter converter = new ListConverter(type, NTList(NTAny), expressionEvaluator(version));
        String mapString = "[42, [42, 1337]]";

        // When
        DefaultParameterValue converted = converter.apply(mapString);

        // Then
        List<Object> list = (List<Object>) converted.value();
        assertThat(list.get(0)).isEqualTo(42L);
        assertThat(list.get(1)).isEqualTo(asList(42L, 1337L));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldFailOnInvalidMixedTypes(CypherVersion version) {
        // Given
        ListConverter converter = new ListConverter(Long.class, NTInteger, expressionEvaluator(version));
        String listString = "[1337, 'forty-two']";

        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> converter.apply(listString));
        assertThat(exception.getMessage()).isEqualTo("Expects a list of Long but got a list of String");
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldPassOnValidMixedTypes(CypherVersion version) {
        // Given
        ListConverter converter = new ListConverter(Object.class, NTAny, expressionEvaluator(version));
        String listString = "[1337, 'forty-two']";

        // When
        DefaultParameterValue value = converter.apply(listString);

        // Then
        assertThat(value).isEqualTo(ntList(asList(1337L, "forty-two"), NTAny));
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleListsOfMaps(CypherVersion version) {
        // Given
        ListConverter converter = new ListConverter(Map.class, NTMap, expressionEvaluator(version));
        String mapString = "[{k1: 42}, {k1: 1337}]";

        // When
        DefaultParameterValue converted = converter.apply(mapString);

        // Then
        List<Object> list = (List<Object>) converted.value();
        assertThat(list.get(0)).isEqualTo(map("k1", 42L));
        assertThat(list.get(1)).isEqualTo(map("k1", 1337L));
    }
}
