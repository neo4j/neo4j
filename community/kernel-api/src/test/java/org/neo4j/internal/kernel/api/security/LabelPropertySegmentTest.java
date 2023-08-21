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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.of;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.values.storable.Values;

public class LabelPropertySegmentTest {

    private static Stream<Arguments> labelPropertySegmentStringRepresentations() {
        return Stream.of(
                of(
                        new LabelPropertySegment("L1", "p1", Values.stringValue("s1"), true),
                        "FOR (n:L1) WHERE n.p1 = 's1'"),
                of(new LabelPropertySegment("p1", Values.stringValue("s1"), true), "FOR (n) WHERE n.p1 = 's1'"),
                of(
                        new LabelPropertySegment("Label Name", "property name", Values.stringValue("s1"), true),
                        "FOR (n:Label Name) WHERE n.property name = 's1'"));
    }

    @Test
    void testConstructorDisallowsNullParameters() {
        Throwable t1 = assertThrows(
                NullPointerException.class, () -> new LabelPropertySegment(null, Values.intValue(1), true));
        assertThat(t1.getMessage()).startsWith("property must not be null");
        Throwable t2 = assertThrows(NullPointerException.class, () -> new LabelPropertySegment("p1", null, true));
        assertThat(t2.getMessage()).startsWith("value must not be null");
    }

    @Test
    void testGetLabel() {
        var lps1 = new LabelPropertySegment("p1", Values.intValue(1), true);
        var lps2 = new LabelPropertySegment("*", "p1", Values.intValue(1), true);
        var lps3 = new LabelPropertySegment("L1", "p1", Values.intValue(1), true);
        assertNull(lps1.label());
        assertNull(lps2.label());
        assertEquals(lps3.label(), "L1");
    }

    @ParameterizedTest()
    @MethodSource("labelPropertySegmentStringRepresentations")
    void testToString(LabelPropertySegment lps, String stringRepresentation) {
        assertEquals(stringRepresentation, lps.toString());
    }
}
