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
import static org.neo4j.internal.kernel.api.security.PropertyRule.NullOperator;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class PropertyRuleTest {

    private static Stream<Arguments> propertyValueRules() {
        return Stream.of(
                of(Values.of(2), Values.of(1), ComparisonOperator.EQUAL, false),
                of(Values.of(1), Values.of(1), ComparisonOperator.EQUAL, true),
                of(Values.NO_VALUE, Values.NO_VALUE, ComparisonOperator.EQUAL, false),
                of(Values.NO_VALUE, Values.of(1), ComparisonOperator.EQUAL, false),
                of(Values.of(1), Values.NO_VALUE, ComparisonOperator.EQUAL, false),
                of(Values.of("one"), Values.of(1), ComparisonOperator.EQUAL, false),
                of(Values.of(1), Values.of("one"), ComparisonOperator.EQUAL, false),
                of(Values.of(2), Values.of(1), ComparisonOperator.NOT_EQUAL, true),
                of(Values.of(1), Values.of(1), ComparisonOperator.NOT_EQUAL, false),
                of(Values.NO_VALUE, Values.NO_VALUE, ComparisonOperator.NOT_EQUAL, false),
                of(Values.NO_VALUE, Values.of(1), ComparisonOperator.NOT_EQUAL, false),
                of(Values.of(1), Values.NO_VALUE, ComparisonOperator.NOT_EQUAL, false),
                of(Values.of("one"), Values.of(1), ComparisonOperator.NOT_EQUAL, true),
                of(Values.of(1), Values.of("one"), ComparisonOperator.NOT_EQUAL, true),
                of(Values.of(1), Values.of(2), ComparisonOperator.GREATER_THAN, false),
                of(Values.of(1), Values.of(1), ComparisonOperator.GREATER_THAN, false),
                of(Values.of(2), Values.of(1), ComparisonOperator.GREATER_THAN, true),
                of(Values.NO_VALUE, Values.NO_VALUE, ComparisonOperator.GREATER_THAN, false),
                of(Values.NO_VALUE, Values.of(1), ComparisonOperator.GREATER_THAN, false),
                of(Values.of(1), Values.NO_VALUE, ComparisonOperator.GREATER_THAN, false),
                of(Values.of("one"), Values.of(1), ComparisonOperator.GREATER_THAN, false),
                of(Values.of(1), Values.of("one"), ComparisonOperator.GREATER_THAN, false),
                of(Values.of(1), Values.of(2), ComparisonOperator.GREATER_THAN_OR_EQUAL, false),
                of(Values.of(1), Values.of(1), ComparisonOperator.GREATER_THAN_OR_EQUAL, true),
                of(Values.of(2), Values.of(1), ComparisonOperator.GREATER_THAN_OR_EQUAL, true),
                of(Values.NO_VALUE, Values.NO_VALUE, ComparisonOperator.GREATER_THAN_OR_EQUAL, false),
                of(Values.NO_VALUE, Values.of(1), ComparisonOperator.GREATER_THAN_OR_EQUAL, false),
                of(Values.of(1), Values.NO_VALUE, ComparisonOperator.GREATER_THAN_OR_EQUAL, false),
                of(Values.of("one"), Values.of(1), ComparisonOperator.GREATER_THAN_OR_EQUAL, false),
                of(Values.of(1), Values.of("one"), ComparisonOperator.GREATER_THAN_OR_EQUAL, false),
                of(Values.of(1), Values.of(2), ComparisonOperator.LESS_THAN, true),
                of(Values.of(1), Values.of(1), ComparisonOperator.LESS_THAN, false),
                of(Values.of(2), Values.of(1), ComparisonOperator.LESS_THAN, false),
                of(Values.NO_VALUE, Values.NO_VALUE, ComparisonOperator.LESS_THAN, false),
                of(Values.NO_VALUE, Values.of(1), ComparisonOperator.LESS_THAN, false),
                of(Values.of(1), Values.NO_VALUE, ComparisonOperator.LESS_THAN, false),
                of(Values.of("one"), Values.of(1), ComparisonOperator.LESS_THAN, false),
                of(Values.of(1), Values.of("one"), ComparisonOperator.LESS_THAN, false),
                of(Values.of(1), Values.of(2), ComparisonOperator.LESS_THAN_OR_EQUAL, true),
                of(Values.of(1), Values.of(1), ComparisonOperator.LESS_THAN_OR_EQUAL, true),
                of(Values.of(2), Values.of(1), ComparisonOperator.LESS_THAN_OR_EQUAL, false),
                of(Values.NO_VALUE, Values.NO_VALUE, ComparisonOperator.LESS_THAN_OR_EQUAL, false),
                of(Values.NO_VALUE, Values.of(1), ComparisonOperator.LESS_THAN_OR_EQUAL, false),
                of(Values.of(1), Values.NO_VALUE, ComparisonOperator.LESS_THAN_OR_EQUAL, false),
                of(Values.of("one"), Values.of(1), ComparisonOperator.LESS_THAN_OR_EQUAL, false),
                of(Values.of(1), Values.of("one"), ComparisonOperator.LESS_THAN_OR_EQUAL, false));
    }

    private static Stream<Arguments> nullPropertyRules() {
        return Stream.of(
                of(Values.of(1), NullOperator.IS_NULL, false),
                of(Values.NO_VALUE, NullOperator.IS_NULL, true),
                of(Values.of(1), NullOperator.IS_NOT_NULL, true),
                of(Values.NO_VALUE, NullOperator.IS_NOT_NULL, false));
    }

    @Test
    void testConstructorDisallowsNullValue() {
        assertThatThrownBy(() -> PropertyRule.newRule(1, null, ComparisonOperator.EQUAL))
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
    void nullPropertyRules(Value nodePropertyValue, NullOperator operator, Boolean expectedResult) {
        final var assertRule = assertThat(PropertyRule.newNullRule(1, operator));
        if (expectedResult) {
            assertRule.accepts(nodePropertyValue);
        } else {
            assertRule.rejects(nodePropertyValue);
        }
    }
}
