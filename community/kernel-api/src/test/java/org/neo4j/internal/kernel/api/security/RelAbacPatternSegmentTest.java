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
import static org.neo4j.internal.kernel.api.security.PatternSegment.RelAbacPatternSegment;

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator;
import org.neo4j.values.storable.Values;

public class RelAbacPatternSegmentTest {

    private static final String USER_TAGS = "abac.native.user_tags";

    @Test
    void testPattern() {
        var aps = new RelAbacPatternSegment("p", USER_TAGS, ComparisonOperator.IN);
        var apsSpy = spy(aps);
        when(apsSpy.propertyString("r")).thenReturn("propertyString");
        assertThat(apsSpy.pattern()).isEqualTo("()-[r]-() WHERE propertyString IN abac.native.user_tags()");
    }

    @Test
    void testPatternWithElementTypes() {
        var aps = new RelAbacPatternSegment(Set.of("KNOWS"), "level", USER_TAGS, ComparisonOperator.IN);
        assertThat(aps.pattern()).isEqualTo("()-[r:KNOWS]-() WHERE r.level IN abac.native.user_tags()");
    }

    @Test
    void testToString() {
        var aps = new RelAbacPatternSegment("p", USER_TAGS, ComparisonOperator.IN);
        var apsSpy = spy(aps);
        when(apsSpy.pattern()).thenReturn("patternString");
        assertThat(apsSpy.toString()).isEqualTo("FOR(patternString)");
    }

    @Test
    void testBuildPropertyRuleWithoutValueThrows() {
        var aps = new RelAbacPatternSegment("p", USER_TAGS, ComparisonOperator.IN);
        assertThatThrownBy(() -> aps.buildPropertyRule(1)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testBuildPropertyRuleWithResolvedValue() {
        var aps = new RelAbacPatternSegment("p", USER_TAGS, ComparisonOperator.IN);
        var tags = Values.stringArray("a", "b");
        assertThat(aps.buildPropertyRule(1, tags)).isEqualTo(PropertyRule.newRule(1, tags, ComparisonOperator.IN));
    }

    @Test
    void testConstructorDisallowsNullParameters() {
        assertThatThrownBy(() -> new RelAbacPatternSegment(null, "p1", USER_TAGS, ComparisonOperator.IN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("elementTypes must not be null");
        assertThatThrownBy(() -> new RelAbacPatternSegment(null, USER_TAGS, ComparisonOperator.IN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("property must not be null");
        assertThatThrownBy(() -> new RelAbacPatternSegment("p1", null, ComparisonOperator.IN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("functionName must not be null");
    }

    @Test
    void testConstructorDisallowsOperatorsOtherThanIn() {
        // NOT_IN included deliberately: negating a per-user attribute list is rejected when the privilege is
        // granted, so no negated abac segment should ever be constructible.
        for (ComparisonOperator operator :
                new ComparisonOperator[] {ComparisonOperator.NOT_IN, ComparisonOperator.EQUAL}) {
            assertThatThrownBy(() -> new PatternSegment.RelAbacPatternSegment("p1", USER_TAGS, operator))
                    .as(operator.name())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("operator must be IN");
        }
    }

    @Test
    void testGetRelationshipTypes() {
        var aps1 = new RelAbacPatternSegment("p1", USER_TAGS, ComparisonOperator.IN);
        var aps2 = new RelAbacPatternSegment(Set.of("T1"), "p1", USER_TAGS, ComparisonOperator.IN);
        var aps3 = new RelAbacPatternSegment(Set.of("T1", "T2"), "p1", USER_TAGS, ComparisonOperator.IN);
        assertThat(aps1.elementTypes()).isEmpty();
        assertThat(aps2.elementTypes()).containsExactlyInAnyOrder("T1");
        assertThat(aps3.elementTypes()).containsExactlyInAnyOrder("T1", "T2");
    }
}
