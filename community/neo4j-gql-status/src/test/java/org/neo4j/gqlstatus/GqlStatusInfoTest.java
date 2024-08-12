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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class GqlStatusInfoTest {

    @Test
    void shouldSubstituteParams() {
        String m = "My name is `$name`, nice to meet `$who`";
        String resultMap = GqlStatusInfoCodes.getMessage(m, Map.of("name", "Neo", "who", "you"));
        String resultList = GqlStatusInfoCodes.getMessage(m, List.of("Neo", "you"));
        assertEquals("My name is `Neo`, nice to meet `you`", resultMap);
        assertEquals(resultMap, resultList);
    }

    @Test
    void shouldNotCrashIfTooFewParams() {
        String m1 = "My name is `$name`, nice to meet `$who`";
        String resultMap1 = GqlStatusInfoCodes.getMessage(m1, Map.of("name", "Neo"));
        String resultList1 = GqlStatusInfoCodes.getMessage(m1, List.of("Neo"));
        assertEquals("My name is `Neo`, nice to meet `$who`", resultMap1);
        assertEquals(resultMap1, resultList1);

        String m2 = "My name is `$name`, nice to meet `$who`";
        String resultMap2 = GqlStatusInfoCodes.getMessage(m2, Map.of("who", "you"));
        String resultList2 = GqlStatusInfoCodes.getMessage(m2, List.of("you"));
        assertEquals("My name is `$name`, nice to meet `you`", resultMap2);
        // If using the "List-way" of setting params, they are replaced in order
        // While the "Map-way" looks up which param to substitute
        assertNotEquals(resultMap2, resultList2);
        assertEquals(resultList2, "My name is `you`, nice to meet `$who`");
    }

    @Test
    void shouldNotCrashIfTooManyParams() {
        String m1 = "My name is `$name`, nice to meet `$who`";
        String resultMap1 = GqlStatusInfoCodes.getMessage(m1, Map.of("name", "Neo", "who", "you", "too", "many"));
        String resultList1 = GqlStatusInfoCodes.getMessage(m1, List.of("Neo", "you", "many"));
        assertEquals("My name is `Neo`, nice to meet `you`", resultMap1);
        assertEquals(resultMap1, resultList1);

        String m2 = "My name is `$name`, nice to meet `$who`";
        String resultMap2 = GqlStatusInfoCodes.getMessage(m2, Map.of("name", "Neo", "who", "you", "too", "many"));
        String resultList2 = GqlStatusInfoCodes.getMessage(m2, List.of("many", "Neo", "you"));
        assertEquals("My name is `Neo`, nice to meet `you`", resultMap2);
        // If using the "List-way" of setting params, they are replaced in order
        // While the "Map-way" looks up which param to substitute
        assertNotEquals(resultMap2, resultList2);
        assertEquals(resultList2, "My name is `many`, nice to meet `Neo`");
    }

    @Test
    void shouldNotCrashIfParamsAreWrong() {
        String m = "My name is `$name`, nice to meet `$who`";
        String resultMap = GqlStatusInfoCodes.getMessage(m, Map.of("abc", "123", "greetings", "hello"));
        String resultList = GqlStatusInfoCodes.getMessage(m, List.of("123", "hello"));
        assertEquals("My name is `$name`, nice to meet `$who`", resultMap);
        // The "List-way" can't know that we don't intend to replace $name and $who
        assertEquals("My name is `123`, nice to meet `hello`", resultList);
        assertNotEquals(resultMap, resultList);
    }

    @Test
    void shouldNotCrashIfParamValueContainsRegexSyntax() {
        // We should actually never send in dollar, but it is special syntax in Regex and we should handle it
        String m1 = "My name is `$name`, nice to meet `$who`";
        String resultMap1 = GqlStatusInfoCodes.getMessage(m1, Map.of("name", "$Neo", "who", "$you"));
        String resultList1 = GqlStatusInfoCodes.getMessage(m1, List.of("$Neo", "$you"));
        assertEquals("My name is `$Neo`, nice to meet `$you`", resultMap1);
        assertEquals(resultMap1, resultList1);

        String m2 = "My name is `$name`, nice to meet `$who`";
        String resultMap2 = GqlStatusInfoCodes.getMessage(m2, Map.of("name", "[A-Z]", "who", "^."));
        String resultList2 = GqlStatusInfoCodes.getMessage(m2, List.of("[A-Z]", "^."));
        assertEquals("My name is `[A-Z]`, nice to meet `^.`", resultMap2);
        assertEquals(resultMap2, resultList2);
    }

    @Test
    void shouldSubstituteRepeatedParams() {
        String m = "The database `$db` wasn't found. Verify that `$db` is correctly spelled";
        String expected =
                "The database `My awesome database` wasn't found. Verify that `My awesome database` is correctly spelled";
        String resultMap = GqlStatusInfoCodes.getMessage(m, Map.of("db", "My awesome database"));
        String resultList = GqlStatusInfoCodes.getMessage(m, List.of("My awesome database", "My awesome database"));
        assertEquals(expected, resultMap);
        assertEquals(resultMap, resultList);
    }

    @Test
    void shouldSubstituteParamsWithoutBackticks() {
        // Missing backticks before dollar
        String m = "My name is $name";
        String expected = "My name is Neo";
        String resultMap = GqlStatusInfoCodes.getMessage(m, Map.of("name", "Neo"));
        String resultList = GqlStatusInfoCodes.getMessage(m, List.of("Neo"));
        assertEquals(expected, resultMap);
        assertEquals(resultList, resultMap);
    }

    @Test
    void shouldNotSubstituteIfNoDollar() {
        // Missing dollar after backticks
        String m = "My `name` is";
        String expected = "My `name` is";
        String resultMap = GqlStatusInfoCodes.getMessage(m, Map.of("name", "Neo"));
        String resultList = GqlStatusInfoCodes.getMessage(m, List.of("Neo"));
        assertEquals(expected, resultMap);
        assertEquals(resultList, resultMap);
    }

    @Test
    void shouldAllowDollarsIfMessageNotCamelCase() {
        // in "$5", the string "5" is not considered valid camelCase
        // the params has to start with a lowercase letter
        // this means that it should not be substituted
        String m1 = "This costs $5, your balance is $`$balance`";
        String expected = "This costs $5, your balance is $`10`";
        String resultMap1 = GqlStatusInfoCodes.getMessage(m1, Map.of("balance", "10"));
        String resultList1 = GqlStatusInfoCodes.getMessage(m1, List.of("10"));
        assertEquals(expected, resultMap1);
        assertEquals(resultMap1, resultList1);

        String m2 = "This costs $9.99, your balance is $`$balance`";
        String expected2 = "This costs $9.99, your balance is $`10`";
        String resultMap2 = GqlStatusInfoCodes.getMessage(m2, Map.of("balance", "10"));
        String resultList2 = GqlStatusInfoCodes.getMessage(m2, List.of("10"));
        assertEquals(expected2, resultMap2);
        assertEquals(resultMap2, resultList2);

        // Without backticks around "balance", and double $
        String m3 = "This costs $9.99, your balance is $$balance";
        String expected3 = "This costs $9.99, your balance is $10";
        String resultMap3 = GqlStatusInfoCodes.getMessage(m3, Map.of("balance", "10"));
        String resultList3 = GqlStatusInfoCodes.getMessage(m3, List.of("10"));
        assertEquals(expected3, resultMap3);
        assertEquals(resultMap3, resultList3);
    }
}
