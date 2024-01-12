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
import static org.neo4j.internal.kernel.api.security.PatternSegment.NullPatternSegment;
import static org.neo4j.internal.kernel.api.security.PropertyRule.NullOperator;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class NullPatternSegmentTest {
    private static Stream<Arguments> patterns() {
        return Arrays.stream(NullOperator.values())
                .flatMap(op -> Stream.of(
                        of(
                                new NullPatternSegment(Set.of("L1"), "p1", op),
                                String.format("(n:L1) WHERE n.p1 %s", op.getSymbol())),
                        of(
                                new NullPatternSegment(Set.of("L1", "L2"), "p1", op),
                                String.format("(n:L1|L2) WHERE n.p1 %s", op.getSymbol())),
                        of(new NullPatternSegment("p1", op), String.format("(n) WHERE n.p1 %s", op.getSymbol())),
                        of(
                                new NullPatternSegment(Set.of("Label Name"), "property name", op),
                                String.format("(n:Label Name) WHERE n.property name %s", op.getSymbol()))));
    }

    @ParameterizedTest
    @MethodSource
    void patterns(NullPatternSegment nps, String pattern) {
        assertThat(nps.pattern()).isEqualTo(pattern);
    }

    @ParameterizedTest
    @MethodSource("patterns")
    void toStringTest(NullPatternSegment nps, String pattern) {
        assertThat(nps.toString()).isEqualTo(String.format("FOR(%s)", pattern));
    }

    @Test
    void testConstructorDisallowsNullParameters() {
        assertThatThrownBy(() -> new PatternSegment.NullPatternSegment(null, "p1", NullOperator.IS_NULL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("labels must not be null");
        assertThatThrownBy(() -> new PatternSegment.NullPatternSegment(null, NullOperator.IS_NULL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("property must not be null");
    }

    @Test
    void testGetLabel() {
        var nps1 = new NullPatternSegment("p1", NullOperator.IS_NULL);
        var nps2 = new NullPatternSegment(Set.of("L1"), "p1", NullOperator.IS_NULL);
        var nps3 = new NullPatternSegment(Set.of("L1", "L2"), "p1", NullOperator.IS_NULL);
        assertThat(nps1.labels()).isEmpty();
        assertThat(nps2.labels()).containsExactlyInAnyOrder("L1");
        assertThat(nps3.labels()).containsExactlyInAnyOrder("L1", "L2");
    }
}
