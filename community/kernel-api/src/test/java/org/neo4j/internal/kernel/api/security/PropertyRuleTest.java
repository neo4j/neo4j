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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.values.storable.Value;

public class PropertyRuleTest {

    private static Stream<Arguments> propertyRules() {
        var value1a = mock(Value.class);
        var value1b = mock(Value.class);
        var value2a = mock(Value.class);

        when(value1a.equals(value1b)).thenReturn(true);
        when(value1b.equals(value1a)).thenReturn(true);
        when(value1a.equals(value2a)).thenReturn(false);
        when(value1b.equals(value2a)).thenReturn(false);
        when(value2a.equals(value1a)).thenReturn(false);
        when(value2a.equals(value1b)).thenReturn(false);

        return Stream.of(
                of("Values don't match and equals = true", value1a, value2a, true, false),
                of("Values don't match and equals = false", value1a, value2a, false, true),
                of("Values do match and equals = true", value1a, value1b, true, true),
                of("Values do match and equals = false", value1a, value1b, false, false));
    }

    @Test
    void testConstructorDisallowsNullValue() {
        Throwable t = assertThrows(NullPointerException.class, () -> PropertyRule.newRule(1, null, true));
        assertThat(t.getMessage()).startsWith("value must not be null");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("propertyRules")
    @DisplayName("Test the PropertyRule's implementation of Predicate.test")
    void predicate(String name, Value v1, Value v2, Boolean equals, Boolean expectedResult) {
        assertEquals(expectedResult, PropertyRule.newRule(1, v1, equals).test(v2));
    }
}
