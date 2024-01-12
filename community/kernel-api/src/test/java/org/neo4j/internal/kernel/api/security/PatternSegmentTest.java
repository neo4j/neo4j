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
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PatternSegmentTest {
    private static Arguments[] labelsStrings() {
        return new Arguments[] {
            of(Set.of("L1"), ":L1"), of(Set.of("L1", "L2"), ":L1|L2"), of(Set.of(), ""),
        };
    }

    @ParameterizedTest
    @MethodSource
    void labelsStrings(Set<String> labels, String stringRepresentation) {
        var ps = spy(PatternSegment.class);
        when(ps.labels()).thenReturn(labels);
        assertThat(ps.labelsString()).isEqualTo(stringRepresentation);
    }

    @Test
    void nodeString() {
        var ps = spy(PatternSegment.class);
        when(ps.labelsString()).thenReturn(":L1");
        assertThat(ps.nodeString()).isEqualTo("(n:L1)");
    }

    @Test
    void propertyString() {
        var ps = spy(PatternSegment.class);
        when(ps.property()).thenReturn("p1");
        assertThat(ps.propertyString()).isEqualTo("n.p1");
    }

    @Test
    void toCypherSnippet() {
        var ps = spy(PatternSegment.class);
        when(ps.pattern()).thenReturn("pattern1");
        assertThat(ps.toCypherSnippet()).isEqualTo("FOR pattern1");
    }
}
