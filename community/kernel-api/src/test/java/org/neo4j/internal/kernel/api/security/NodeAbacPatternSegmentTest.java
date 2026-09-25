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
import static org.neo4j.internal.kernel.api.security.PatternSegment.NodeAbacPatternSegment;

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator;
import org.neo4j.values.storable.Values;

public class NodeAbacPatternSegmentTest {

    private static final String USER_TAGS = "abac.native.user_tags";

    @Test
    void testPattern() {
        var aps = new NodeAbacPatternSegment("p", USER_TAGS, ComparisonOperator.IN);
        var apsSpy = spy(aps);
        when(apsSpy.propertyString("n")).thenReturn("propertyString");
        assertThat(apsSpy.pattern()).isEqualTo("(n) WHERE propertyString IN abac.native.user_tags()");
    }

    @Test
    void testPatternWithElementTypes() {
        var aps = new NodeAbacPatternSegment(Set.of("Document"), "level", USER_TAGS, ComparisonOperator.IN);
        assertThat(aps.pattern()).isEqualTo("(n:Document) WHERE n.level IN abac.native.user_tags()");
    }

    @Test
    void testToCypherSnippet() {
        var aps = new NodeAbacPatternSegment(Set.of("Document"), "level", USER_TAGS, ComparisonOperator.IN);
        assertThat(aps.toCypherSnippet()).isEqualTo("FOR (n:Document) WHERE n.level IN abac.native.user_tags()");
    }

    @Test
    void testToString() {
        var aps = new NodeAbacPatternSegment("p", USER_TAGS, ComparisonOperator.IN);
        var apsSpy = spy(aps);
        when(apsSpy.pattern()).thenReturn("patternString");
        assertThat(apsSpy.toString()).isEqualTo("FOR(patternString)");
    }

    @Test
    void testBuildPropertyRuleWithoutValueThrows() {
        var aps = new NodeAbacPatternSegment("p", USER_TAGS, ComparisonOperator.IN);
        assertThatThrownBy(() -> aps.buildPropertyRule(1)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testBuildPropertyRuleWithResolvedValue() {
        var aps = new NodeAbacPatternSegment("p", USER_TAGS, ComparisonOperator.IN);
        var tags = Values.stringArray("a", "b");
        assertThat(aps.buildPropertyRule(1, tags)).isEqualTo(PropertyRule.newRule(1, tags, ComparisonOperator.IN));
    }

    @Test
    void testBuildPropertyRuleWithEmptyResolvedValue() {
        var aps = new NodeAbacPatternSegment("p", USER_TAGS, ComparisonOperator.IN);
        var noTags = Values.stringArray(new String[0]);
        assertThat(aps.buildPropertyRule(1, noTags)).isEqualTo(PropertyRule.newRule(1, noTags, ComparisonOperator.IN));
    }

    @Test
    void testConstructorDisallowsNullParameters() {
        assertThatThrownBy(() -> new NodeAbacPatternSegment(null, "p1", USER_TAGS, ComparisonOperator.IN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("elementTypes must not be null");
        assertThatThrownBy(() -> new NodeAbacPatternSegment(null, USER_TAGS, ComparisonOperator.IN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("property must not be null");
        assertThatThrownBy(() -> new NodeAbacPatternSegment("p1", null, ComparisonOperator.IN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("functionName must not be null");
    }

    @Test
    void testConstructorDisallowsOperatorsOtherThanIn() {
        // NOT_IN included deliberately: negating a per-user attribute list is rejected when the privilege is
        // granted, so no negated abac segment should ever be constructible.
        for (ComparisonOperator operator :
                new ComparisonOperator[] {ComparisonOperator.NOT_IN, ComparisonOperator.EQUAL}) {
            assertThatThrownBy(() -> new NodeAbacPatternSegment("p1", USER_TAGS, operator))
                    .as(operator.name())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("operator must be IN");
        }
    }

    @Test
    void testGetLabel() {
        var aps1 = new NodeAbacPatternSegment("p1", USER_TAGS, ComparisonOperator.IN);
        var aps2 = new NodeAbacPatternSegment(Set.of("L1"), "p1", USER_TAGS, ComparisonOperator.IN);
        var aps3 = new NodeAbacPatternSegment(Set.of("L1", "L2"), "p1", USER_TAGS, ComparisonOperator.IN);
        assertThat(aps1.elementTypes()).isEmpty();
        assertThat(aps2.elementTypes()).containsExactlyInAnyOrder("L1");
        assertThat(aps3.elementTypes()).containsExactlyInAnyOrder("L1", "L2");
    }
}
