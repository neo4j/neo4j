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

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.cypher.internal.evaluator.Evaluator.expressionEvaluator;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntMap;

import java.util.List;
import java.util.Map;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;

class MapConverterTest {

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleNullString(CypherVersion version) {
        assertConvert(version, "null").isEqualTo(ntMap(null));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleEmptyMap(CypherVersion version) {
        assertConvert(version, "{}").isEqualTo(ntMap(emptyMap()));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleEmptyMapWithSpaces(CypherVersion version) {
        assertConvert(version, " {  }  ").isEqualTo(ntMap(emptyMap()));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleSingleQuotedValue(CypherVersion version) {
        assertConvert(version, "{key: 'value'}").isEqualTo(ntMap(map("key", "value")));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleEscapedSingleQuotedInValue2(CypherVersion version) {
        assertConvert(version, "{key: \"va\'lue\"}").isEqualTo(ntMap(map("key", "va\'lue")));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleEscapedDoubleQuotedInValue1(CypherVersion version) {
        assertConvert(version, "{key: \"va\\\"lue\"}").isEqualTo(ntMap(map("key", "va\"lue")));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleEscapedDoubleQuotedInValue2(CypherVersion version) {
        assertConvert(version, "{key: 'va\"lue'}").isEqualTo(ntMap(map("key", "va\"lue")));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleDoubleQuotedValue(CypherVersion version) {
        assertConvert(version, "{key: \"value\"}").isEqualTo(ntMap(map("key", "value")));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleKeyWithEscapedSingleQuote(CypherVersion version) {
        assertConvert(version, "{`k\'ey`: \"value\"}").isEqualTo(ntMap(map("k\'ey", "value")));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleKeyWithEscapedDoubleQuote(CypherVersion version) {
        assertConvert(version, "{`k\"ey`: \"value\"}").isEqualTo(ntMap(map("k\"ey", "value")));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleIntegerValue(CypherVersion version) {
        assertConvert(version, "{key: 1337}").isEqualTo(ntMap(map("key", 1337L)));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleFloatValue(CypherVersion version) {
        assertConvert(version, "{key: 2.718281828}").isEqualTo(ntMap(map("key", 2.718281828)));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleNullValue(CypherVersion version) {
        assertConvert(version, "{key: null}").isEqualTo(ntMap(map("key", null)));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleFalseValue(CypherVersion version) {
        assertConvert(version, "{key: false}").isEqualTo(ntMap(map("key", false)));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleTrueValue(CypherVersion version) {
        assertConvert(version, "{key: true}").isEqualTo(ntMap(map("key", true)));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleMultipleKeys(CypherVersion version) {
        assertConvert(version, "{k1: 2.718281828, k2: 'e'}").isEqualTo(ntMap(map("k1", 2.718281828, "k2", "e")));
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleNestedMaps(CypherVersion version) {
        assertConvert(version, "{k1: 1337, k2: { k1 : 1337, k2: {k1: 1337}}}")
                .isEqualTo(ntMap(Map.of("k1", 1337L, "k2", Map.of("k1", 1337L, "k2", Map.of("k1", 1337L)))));
    }

    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldFailOnMalformedMap(CypherVersion version) {
        assertThatThrownBy(() -> convert(version, "{k1: 2.718281828, k2: 'e'}}"))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @EnumSource(CypherVersion.class)
    void shouldHandleMapsWithLists(CypherVersion version) {
        assertConvert(version, "{k1: [1337, 42]}").isEqualTo(ntMap(Map.of("k1", List.of(1337L, 42L))));
    }

    private DefaultParameterValue convert(CypherVersion version, String value) {
        return new MapConverter(expressionEvaluator(version)).apply(value);
    }

    private ObjectAssert<DefaultParameterValue> assertConvert(CypherVersion version, String value) {
        return assertThat(convert(version, value));
    }
}
