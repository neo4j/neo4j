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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.security.PatternSegment.NullPatternSegment;
import static org.neo4j.internal.kernel.api.security.PropertyRule.NullOperator;

import java.util.Set;
import org.junit.jupiter.api.Test;

public class NullPatternSegmentTest {

    @Test
    void testPattern() {
        var nps = new NullPatternSegment("p", NullOperator.IS_NULL);
        var npsSpy = spy(nps);
        when(npsSpy.propertyString()).thenReturn("propertyString");
        assertThat(npsSpy.pattern()).isEqualTo("(n) WHERE propertyString IS NULL");
    }

    @Test
    void testToString() {
        var nps = new NullPatternSegment("propertyString", NullOperator.IS_NULL);
        var npsSpy = spy(nps);
        when(npsSpy.pattern()).thenReturn("patternString");
        assertThat(npsSpy.toString()).isEqualTo("FOR(patternString)");
    }

    @Test
    void testConstructorDisallowsNullParameters() {
        assertThatThrownBy(() -> new NullPatternSegment(null, "p1", NullOperator.IS_NULL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("labels must not be null");
        assertThatThrownBy(() -> new NullPatternSegment(null, NullOperator.IS_NULL))
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
