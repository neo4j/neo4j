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
package org.neo4j.gqlstatus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class GqlStatusInfoTest {

    @Test
    public void test() {
        String m1 = "My name is `$name`";
        String result = GqlStatusInfoCodes.STATUS_00000.toJavaFormattable(m1);
        assertEquals("My name is `%s`", result);
        String m2 = "My name is `$name`.";
        result = GqlStatusInfoCodes.STATUS_00000.toJavaFormattable(m2);
        assertEquals("My name is `%s`.", result);
        String m3 = "My name is `$name` and `$name2`";
        result = GqlStatusInfoCodes.STATUS_00000.toJavaFormattable(m3);
        assertEquals("My name is `%s` and `%s`", result);
    }
}
