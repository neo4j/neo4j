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
import static org.neo4j.internal.kernel.api.security.PatternSegment.ValuePatternSegment;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator;
import org.neo4j.values.storable.Values;

public class ValuePatternSegmentTest {
    private static Stream<Arguments> patterns() {
        return Arrays.stream(ComparisonOperator.values())
                .flatMap(op -> Stream.of(
                        of(
                                new ValuePatternSegment(Set.of("L1"), "p1", Values.stringValue("s1"), op),
                                String.format("(n:L1) WHERE n.p1 %s 's1'", op.getSymbol())),
                        of(
                                new ValuePatternSegment(Set.of("L1", "L2"), "p1", Values.stringValue("s1"), op),
                                String.format("(n:L1|L2) WHERE n.p1 %s 's1'", op.getSymbol())),
                        of(
                                new ValuePatternSegment("p1", Values.stringValue("s1"), op),
                                String.format("(n) WHERE n.p1 %s 's1'", op.getSymbol())),
                        of(
                                new ValuePatternSegment(
                                        Set.of("Label Name"), "property name", Values.stringValue("s1"), op),
                                String.format("(n:Label Name) WHERE n.property name %s 's1'", op.getSymbol()))));
    }

    @ParameterizedTest
    @MethodSource
    void patterns(ValuePatternSegment vps, String pattern) {
        assertThat(vps.pattern()).isEqualTo(pattern);
    }

    @ParameterizedTest
    @MethodSource("patterns")
    void toStringTest(ValuePatternSegment vps, String pattern) {
        assertThat(vps.toString()).isEqualTo(String.format("FOR(%s)", pattern));
    }

    @Test
    void testConstructorDisallowsNullParameters() {
        assertThatThrownBy(() -> new ValuePatternSegment(null, "p1", Values.intValue(1), ComparisonOperator.EQUAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("labels must not be null");
        assertThatThrownBy(() -> new ValuePatternSegment(null, Values.intValue(1), ComparisonOperator.EQUAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("property must not be null");
        assertThatThrownBy(() -> new ValuePatternSegment("p1", null, ComparisonOperator.EQUAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("value must not be null");
        assertThatThrownBy(() -> new ValuePatternSegment("p1", Values.NO_VALUE, ComparisonOperator.EQUAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("value must not be NO_VALUE. Use NullPatternSegment for this purpose.");
    }

    @Test
    void testGetLabel() {
        var vps1 = new ValuePatternSegment("p1", Values.intValue(1), ComparisonOperator.EQUAL);
        var vps2 = new ValuePatternSegment(Set.of("L1"), "p1", Values.intValue(1), ComparisonOperator.EQUAL);
        var vps3 = new ValuePatternSegment(Set.of("L1", "L2"), "p1", Values.intValue(1), ComparisonOperator.EQUAL);
        assertThat(vps1.labels()).isEmpty();
        assertThat(vps2.labels()).containsExactlyInAnyOrder("L1");
        assertThat(vps3.labels()).containsExactlyInAnyOrder("L1", "L2");
    }
}
