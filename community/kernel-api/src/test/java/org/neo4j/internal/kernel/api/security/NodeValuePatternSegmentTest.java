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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.security.PatternSegment.NodeValuePatternSegment;

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class NodeValuePatternSegmentTest {

    @Test
    void testPattern() {
        var value = mock(Value.class);
        when(value.prettyPrint()).thenReturn("valueString");
        var vps = new NodeValuePatternSegment("p", value, ComparisonOperator.EQUAL);
        var vpsSpy = spy(vps);
        when(vpsSpy.propertyString("n")).thenReturn("propertyString");
        assertThat(vpsSpy.pattern()).isEqualTo("(n) WHERE propertyString = valueString");
    }

    @Test
    void testPatternForValueInListProperty() {
        var value = mock(Value.class);
        when(value.prettyPrint()).thenReturn("valueString");
        var vps = new NodeValuePatternSegment("p", value, ComparisonOperator.VALUE_IN_LIST_PROPERTY);
        var vpsSpy = spy(vps);
        when(vpsSpy.propertyString("n")).thenReturn("propertyString");
        assertThat(vpsSpy.pattern()).isEqualTo("(n) WHERE valueString IN propertyString");
    }

    @Test
    void testPatternForNegatedValueInListProperty() {
        var value = mock(Value.class);
        when(value.prettyPrint()).thenReturn("valueString");
        var vps = new NodeValuePatternSegment("p", value, ComparisonOperator.NOT_VALUE_IN_LIST_PROPERTY);
        var vpsSpy = spy(vps);
        when(vpsSpy.propertyString("n")).thenReturn("propertyString");
        assertThat(vpsSpy.pattern()).isEqualTo("(n) WHERE NOT valueString IN propertyString");
    }

    @Test
    void testToString() {
        var vps = new NodeValuePatternSegment("propertyString", mock(Value.class), ComparisonOperator.EQUAL);
        var vpsSpy = spy(vps);
        when(vpsSpy.pattern()).thenReturn("patternString");
        assertThat(vpsSpy.toString()).isEqualTo("FOR(patternString)");
    }

    @Test
    void testConstructorDisallowsNullParameters() {
        assertThatThrownBy(() -> new NodeValuePatternSegment(null, "p1", Values.intValue(1), ComparisonOperator.EQUAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("elementTypes must not be null");
        assertThatThrownBy(() -> new NodeValuePatternSegment(null, Values.intValue(1), ComparisonOperator.EQUAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("property must not be null");
        assertThatThrownBy(() -> new NodeValuePatternSegment("p1", null, ComparisonOperator.EQUAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("value must not be null");
        assertThatThrownBy(() -> new NodeValuePatternSegment("p1", Values.NO_VALUE, ComparisonOperator.EQUAL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("value must not be NO_VALUE. Use NullPatternSegment for this purpose.");
    }

    @Test
    void testGetLabel() {
        var vps1 = new NodeValuePatternSegment("p1", Values.intValue(1), ComparisonOperator.EQUAL);
        var vps2 = new NodeValuePatternSegment(Set.of("L1"), "p1", Values.intValue(1), ComparisonOperator.EQUAL);
        var vps3 = new NodeValuePatternSegment(Set.of("L1", "L2"), "p1", Values.intValue(1), ComparisonOperator.EQUAL);
        assertThat(vps1.elementTypes()).isEmpty();
        assertThat(vps2.elementTypes()).containsExactlyInAnyOrder("L1");
        assertThat(vps3.elementTypes()).containsExactlyInAnyOrder("L1", "L2");
    }
}
