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
package org.neo4j.internal.kernel.api.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator;
import static org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator.EQUAL;
import static org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator.GREATER_THAN;
import static org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator.IN;
import static org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator.LESS_THAN;
import static org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator.NOT_EQUAL;
import static org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator.NOT_IN;
import static org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator.NOT_VALUE_IN_LIST_PROPERTY;
import static org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator.VALUE_IN_LIST_PROPERTY;
import static org.neo4j.internal.kernel.api.security.PropertyRule.NullOperator;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class PropertyRuleTest {

    private static Stream<Arguments> propertyValueRules() {
        return Stream.of(
                of(Values.of(2), Values.of(1), EQUAL, false),
                of(Values.of(1), Values.of(1), EQUAL, true),
                of(Values.NO_VALUE, Values.NO_VALUE, EQUAL, false),
                of(Values.NO_VALUE, Values.of(1), EQUAL, false),
                of(Values.of(1), Values.NO_VALUE, EQUAL, false),
                of(Values.of("one"), Values.of(1), EQUAL, false),
                of(Values.of(1), Values.of("one"), EQUAL, false),
                of(DateValue.date(2024, 10, 11), DateValue.date(2024, 10, 11), EQUAL, true),
                of(DateValue.date(2024, 10, 11), DateValue.date(2023, 10, 11), EQUAL, false),
                of(Values.of(2), Values.of(1), NOT_EQUAL, true),
                of(Values.of(1), Values.of(1), NOT_EQUAL, false),
                of(Values.NO_VALUE, Values.NO_VALUE, NOT_EQUAL, false),
                of(Values.NO_VALUE, Values.of(1), NOT_EQUAL, false),
                of(Values.of(1), Values.NO_VALUE, NOT_EQUAL, false),
                of(Values.of("one"), Values.of(1), NOT_EQUAL, true),
                of(Values.of(1), Values.of("one"), NOT_EQUAL, true),
                of(
                        DateTimeValue.datetime(2024, 10, 11, 14, 0, 0, 0, "+01:00"),
                        DateTimeValue.datetime(2024, 10, 11, 14, 0, 0, 0, "+01:00"),
                        NOT_EQUAL,
                        false),
                of(
                        DateTimeValue.datetime(2023, 10, 11, 14, 0, 0, 0, "+01:00"),
                        DateTimeValue.datetime(2024, 10, 11, 14, 0, 0, 0, "+01:00"),
                        NOT_EQUAL,
                        true),
                of(Values.of(1), Values.of(2), GREATER_THAN, false),
                of(Values.of(1), Values.of(1), GREATER_THAN, false),
                of(Values.of(2), Values.of(1), GREATER_THAN, true),
                of(Values.NO_VALUE, Values.NO_VALUE, GREATER_THAN, false),
                of(Values.NO_VALUE, Values.of(1), GREATER_THAN, false),
                of(Values.of(1), Values.NO_VALUE, GREATER_THAN, false),
                of(Values.of("one"), Values.of(1), GREATER_THAN, false),
                of(Values.of(1), Values.of("one"), GREATER_THAN, false),
                of(
                        LocalDateTimeValue.localDateTime(2024, 10, 11, 14, 0, 0, 20),
                        LocalDateTimeValue.localDateTime(2024, 10, 11, 14, 0, 0, 20),
                        GREATER_THAN,
                        false),
                of(
                        LocalDateTimeValue.localDateTime(2024, 10, 11, 14, 0, 0, 21),
                        LocalDateTimeValue.localDateTime(2024, 10, 11, 14, 0, 0, 20),
                        GREATER_THAN,
                        true),
                of(
                        PointValue.parse("{x:2, y:2}"),
                        PointValue.parse("{x:1, y:2}"),
                        GREATER_THAN,
                        false), // We do not support comparison of points (they will always be false if they are not
                // equal)
                of(Values.of(1), Values.of(2), GREATER_THAN_OR_EQUAL, false),
                of(Values.of(1), Values.of(1), GREATER_THAN_OR_EQUAL, true),
                of(Values.of(2), Values.of(1), GREATER_THAN_OR_EQUAL, true),
                of(Values.NO_VALUE, Values.NO_VALUE, GREATER_THAN_OR_EQUAL, false),
                of(Values.NO_VALUE, Values.of(1), GREATER_THAN_OR_EQUAL, false),
                of(Values.of(1), Values.NO_VALUE, GREATER_THAN_OR_EQUAL, false),
                of(Values.of("one"), Values.of(1), GREATER_THAN_OR_EQUAL, false),
                of(Values.of(1), Values.of("one"), GREATER_THAN_OR_EQUAL, false),
                of(
                        LocalTimeValue.localTime(14, 0, 0, 20),
                        LocalTimeValue.localTime(14, 0, 0, 21),
                        GREATER_THAN_OR_EQUAL,
                        false),
                of(
                        LocalTimeValue.localTime(14, 0, 0, 20),
                        LocalTimeValue.localTime(14, 0, 0, 20),
                        GREATER_THAN_OR_EQUAL,
                        true),
                of(Values.of(1), Values.of(2), LESS_THAN, true),
                of(Values.of(1), Values.of(1), LESS_THAN, false),
                of(Values.of(2), Values.of(1), LESS_THAN, false),
                of(Values.NO_VALUE, Values.NO_VALUE, LESS_THAN, false),
                of(Values.NO_VALUE, Values.of(1), LESS_THAN, false),
                of(Values.of(1), Values.NO_VALUE, LESS_THAN, false),
                of(Values.of("one"), Values.of(1), LESS_THAN, false),
                of(Values.of(1), Values.of("one"), LESS_THAN, false),
                of(TimeValue.time(14, 0, 0, 20, "+01:00"), TimeValue.time(14, 0, 0, 20, "+01:00"), LESS_THAN, false),
                of(TimeValue.time(14, 0, 0, 20, "+00:00"), TimeValue.time(14, 0, 0, 20, "+01:00"), LESS_THAN, false),
                of(Values.of(1), Values.of(2), LESS_THAN_OR_EQUAL, true),
                of(Values.of(1), Values.of(1), LESS_THAN_OR_EQUAL, true),
                of(Values.of(2), Values.of(1), LESS_THAN_OR_EQUAL, false),
                of(Values.NO_VALUE, Values.NO_VALUE, LESS_THAN_OR_EQUAL, false),
                of(Values.NO_VALUE, Values.of(1), LESS_THAN_OR_EQUAL, false),
                of(Values.of(1), Values.NO_VALUE, LESS_THAN_OR_EQUAL, false),
                of(Values.of("one"), Values.of(1), LESS_THAN_OR_EQUAL, false),
                of(Values.of(1), Values.of("one"), LESS_THAN_OR_EQUAL, false),
                of(
                        DurationValue.duration(10, 10, 10, 10),
                        DurationValue.duration(10, 10, 10, 10),
                        LESS_THAN_OR_EQUAL,
                        true),
                of(
                        DurationValue.duration(10, 10, 10, 9),
                        DurationValue.duration(10, 10, 10, 10),
                        LESS_THAN_OR_EQUAL,
                        false), // can not compare different durations
                of(
                        DurationValue.duration(10, 10, 10, 11),
                        DurationValue.duration(10, 10, 10, 10),
                        LESS_THAN_OR_EQUAL,
                        false),
                of(Values.of(1), Values.of(new String[] {"one", "two"}), IN, false),
                of(Values.of("one"), Values.of(new String[] {"one", "two"}), IN, true),
                of(Values.of(1), Values.of(new int[] {1, 2}), IN, true),
                of(Values.of(1L), Values.of(new int[] {1, 2}), IN, true),
                of(Values.of(1), Values.of(new long[] {1L, 2L}), IN, true),
                of(Values.of(3), Values.of(new int[] {1, 2}), IN, false),
                of(Values.of(1), Values.of(new int[0]), IN, false),
                of(Values.of(1), Values.of("one"), IN, false),
                of(Values.of(1), Values.NO_VALUE, IN, false),
                of(Values.NO_VALUE, Values.NO_VALUE, IN, false),
                of(Values.NO_VALUE, Values.of(new int[] {1, 2}), IN, false),
                of(
                        PointValue.parse("{x:1, y:2}"),
                        Values.of(new PointValue[] {PointValue.parse("{x:1, y:2}"), PointValue.parse("{x:2, y:2}")}),
                        IN,
                        true),
                of(Values.of(1), Values.of(new String[] {"one", "two"}), NOT_IN, true),
                of(Values.of("one"), Values.of(new String[] {"one", "two"}), NOT_IN, false),
                of(Values.of(1), Values.of(new int[] {1, 2}), NOT_IN, false),
                of(Values.of(1L), Values.of(new int[] {1, 2}), NOT_IN, false),
                of(Values.of(1), Values.of(new long[] {1L, 2L}), NOT_IN, false),
                of(Values.of(3), Values.of(new int[] {1, 2}), NOT_IN, true),
                of(Values.of(1), Values.of(new int[0]), NOT_IN, true),
                of(Values.of(1), Values.of("one"), NOT_IN, true),
                of(Values.of(1), Values.NO_VALUE, NOT_IN, false),
                of(Values.NO_VALUE, Values.NO_VALUE, NOT_IN, false),
                of(Values.NO_VALUE, Values.of(new int[] {1, 2}), NOT_IN, false),
                of(
                        PointValue.parse("{x:1, y:2}"),
                        Values.of(new PointValue[] {PointValue.parse("{x:10, y:2}"), PointValue.parse("{x:2, y:2}")}),
                        NOT_IN,
                        true),
                // VALUE_IN_LIST_PROPERTY: <scalar> IN n.prop, with strict-list semantics (property must be a
                // list/array)
                of(Values.of(new String[] {"one", "two"}), Values.of("one"), VALUE_IN_LIST_PROPERTY, true),
                of(Values.of(new String[] {"one", "two"}), Values.of("three"), VALUE_IN_LIST_PROPERTY, false),
                of(Values.of(new int[] {1, 2}), Values.of(1), VALUE_IN_LIST_PROPERTY, true),
                of(Values.of(new int[] {1, 2}), Values.of(1L), VALUE_IN_LIST_PROPERTY, true), // numeric coercion
                of(Values.of(new long[] {1L, 2L}), Values.of(1), VALUE_IN_LIST_PROPERTY, true),
                of(Values.of(new int[] {1, 2}), Values.of(3), VALUE_IN_LIST_PROPERTY, false),
                of(Values.of(new int[0]), Values.of(1), VALUE_IN_LIST_PROPERTY, false), // empty list
                of(
                        Values.of("one"),
                        Values.of("one"),
                        VALUE_IN_LIST_PROPERTY,
                        false), // strict: scalar property never matches
                of(
                        Values.of(1),
                        Values.of(1),
                        VALUE_IN_LIST_PROPERTY,
                        false), // strict: scalar property never matches when equal
                of(Values.NO_VALUE, Values.of(1), VALUE_IN_LIST_PROPERTY, false), // absent property never matches
                of(
                        Values.of(new int[] {1, 2}),
                        Values.NO_VALUE,
                        VALUE_IN_LIST_PROPERTY,
                        false), // null needle never matches
                of(
                        Values.of(new PointValue[] {PointValue.parse("{x:1, y:2}"), PointValue.parse("{x:2, y:2}")}),
                        PointValue.parse("{x:1, y:2}"),
                        VALUE_IN_LIST_PROPERTY,
                        true),
                // NOT_VALUE_IN_LIST_PROPERTY: NOT <scalar> IN n.prop, with the same strict-list semantics
                of(Values.of(new String[] {"one", "two"}), Values.of("three"), NOT_VALUE_IN_LIST_PROPERTY, true),
                of(Values.of(new String[] {"one", "two"}), Values.of("one"), NOT_VALUE_IN_LIST_PROPERTY, false),
                of(Values.of(new int[] {1, 2}), Values.of(3), NOT_VALUE_IN_LIST_PROPERTY, true),
                of(Values.of(new int[] {1, 2}), Values.of(1), NOT_VALUE_IN_LIST_PROPERTY, false),
                of(
                        Values.of(new int[0]),
                        Values.of(1),
                        NOT_VALUE_IN_LIST_PROPERTY,
                        true), // not in empty list, and it is a list
                of(
                        Values.of("one"),
                        Values.of("one"),
                        NOT_VALUE_IN_LIST_PROPERTY,
                        false), // strict: scalar property never matches
                of(
                        Values.NO_VALUE,
                        Values.of(1),
                        NOT_VALUE_IN_LIST_PROPERTY,
                        false), // strict: absent never matches, even negated
                of(
                        Values.of(new int[] {1, 2}),
                        Values.NO_VALUE,
                        NOT_VALUE_IN_LIST_PROPERTY,
                        false)); // null needle: in is NO_VALUE
    }

    private static Stream<Arguments> ValuePredicateStrings() {
        return Stream.of(
                of(EQUAL, "l = r"),
                of(NOT_EQUAL, "l <> r"),
                of(GREATER_THAN, "l > r"),
                of(GREATER_THAN_OR_EQUAL, "l >= r"),
                of(LESS_THAN, "l < r"),
                of(LESS_THAN_OR_EQUAL, "l <= r"),
                of(IN, "l IN r"),
                of(NOT_IN, "NOT l IN r"),
                of(VALUE_IN_LIST_PROPERTY, "r IN l"),
                of(NOT_VALUE_IN_LIST_PROPERTY, "NOT r IN l"));
    }

    private static Stream<Arguments> nullPropertyRules() {
        return Stream.of(
                of(Values.of(1), NullOperator.IS_NULL, false),
                of(Values.NO_VALUE, NullOperator.IS_NULL, true),
                of(Values.of(1), NullOperator.IS_NOT_NULL, true),
                of(Values.NO_VALUE, NullOperator.IS_NOT_NULL, false));
    }

    private static Stream<Arguments> nullPredicateStrings() {
        return Stream.of(of(NullOperator.IS_NULL, "l IS NULL"), of(NullOperator.IS_NOT_NULL, "l IS NOT NULL"));
    }

    @Test
    void testConstructorDisallowsNullValue() {
        assertThatThrownBy(() -> PropertyRule.newRule(1, null, EQUAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("value must not be null");
    }

    @ParameterizedTest
    @MethodSource
    void propertyValueRules(
            Value nodePropertyValue, Value propertyRuleValue, ComparisonOperator operator, Boolean expectedResult) {
        final var assertRule = assertThat(PropertyRule.newRule(1, propertyRuleValue, operator));
        if (expectedResult) {
            assertRule.accepts(nodePropertyValue);
        } else {
            assertRule.rejects(nodePropertyValue);
        }
    }

    @ParameterizedTest
    @MethodSource
    void ValuePredicateStrings(ComparisonOperator operator, String expectedPredicateString) {
        assertThat(operator.toPredicateString("l", "r")).isEqualTo(expectedPredicateString);
    }

    @ParameterizedTest
    @MethodSource
    void nullPropertyRules(Value nodePropertyValue, NullOperator operator, Boolean expectedResult) {
        final var assertRule = assertThat(PropertyRule.newNullRule(1, operator));
        if (expectedResult) {
            assertRule.accepts(nodePropertyValue);
        } else {
            assertRule.rejects(nodePropertyValue);
        }
    }

    @ParameterizedTest
    @MethodSource
    void nullPredicateStrings(NullOperator operator, String expectedPredicateString) {
        assertThat(operator.toPredicateString("l")).isEqualTo(expectedPredicateString);
    }
}
