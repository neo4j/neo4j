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
package org.neo4j.kernel.api.schema;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.neo4j.common.TokenNameLookup;

public final class SchemaTestUtil {
    private SchemaTestUtil() {}

    public static void assertEquality(Object o1, Object o2) {
        assertEquals(o1, o2, o1.getClass().getSimpleName() + "s are not equal");
        assertEquals(o1.hashCode(), o2.hashCode(), o1.getClass().getSimpleName() + "s do not have the same hashcode");
    }

    static void assertArray(int[] values, int... expected) {
        assertThat(values.length).isEqualTo(expected.length);
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], expected[i], format("Expected %d, got %d at index %d", expected[i], values[i], i));
        }
    }

    public static final TokenNameLookup SIMPLE_NAME_LOOKUP = new TokenNameLookup() {
        @Override
        public String labelGetName(int labelId) {
            return "Label" + labelId;
        }

        @Override
        public String relationshipTypeGetName(int relationshipTypeId) {
            return "RelType" + relationshipTypeId;
        }

        @Override
        public String propertyKeyGetName(int propertyKeyId) {
            return "property" + propertyKeyId;
        }
    };
}
