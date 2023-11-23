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

import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.values.storable.Values;

public class PatternSegmentTest {

    private static Stream<Arguments> patternSegmentStringRepresentations() {
        return Stream.of(
                of(
                        new PatternSegment(Set.of("L1"), "p1", Values.stringValue("s1"), true),
                        "FOR (n:L1) WHERE n.p1 = 's1'"),
                of(
                        new PatternSegment(Set.of("L1", "L2"), "p1", Values.stringValue("s1"), true),
                        "FOR (n:L1|L2) WHERE n.p1 = 's1'"),
                of(new PatternSegment("p1", Values.stringValue("s1"), true), "FOR (n) WHERE n.p1 = 's1'"),
                of(
                        new PatternSegment(Set.of("Label Name"), "property name", Values.stringValue("s1"), true),
                        "FOR (n:Label Name) WHERE n.property name = 's1'"));
    }

    @ParameterizedTest()
    @MethodSource
    void patternSegmentStringRepresentations(PatternSegment lps, String stringRepresentation) {
        assertThat(lps.toString()).isEqualTo(stringRepresentation);
    }

    @Test
    void testConstructorDisallowsNullParameters() {
        assertThatThrownBy(() -> new PatternSegment(null, Values.intValue(1), true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("property must not be null");

        assertThatThrownBy(() -> new PatternSegment("p1", null, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("value must not be null");
    }

    @Test
    void testGetLabel() {
        var ps1 = new PatternSegment("p1", Values.intValue(1), true);
        var ps2 = new PatternSegment(Set.of("L1"), "p1", Values.intValue(1), true);
        var ps3 = new PatternSegment(Set.of("L1", "L2"), "p1", Values.intValue(1), true);
        assertThat(ps1.labels()).isEmpty();
        assertThat(ps2.labels()).isEqualTo(Set.of("L1"));
        assertThat(ps3.labels()).isEqualTo(Set.of("L1", "L2"));
    }
}
