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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class GqlStatusTest {
    @Test
    void shouldFormatParams() {
        var statusCode = GqlStatusInfoCodes.STATUS_52U00;
        List<String> paramList = new ArrayList<String>();
        paramList.add("param1");
        paramList.add("param2");
        paramList.add("param3");
        String message = statusCode.getMessage(paramList);
        assertEquals("Execution of the procedure `param1` failed due to `param2`: `param3`", message);
    }

    @Test
    void shouldFailOnEmptyGqlStatus() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> new GqlStatus(""), hint);
        assertEquals(errorMessageStart + "got an empty string.", e.getMessage());
    }

    @Test
    void shouldFailOnTooShortGqlStatus() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> new GqlStatus("125A"), hint);
        assertEquals(errorMessageStart + "got: 125A.", e.getMessage());
    }

    @Test
    void shouldFailOnTooLongGqlStatus() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> new GqlStatus("125ABC"), hint);
        assertEquals(errorMessageStart + "got: 125ABC.", e.getMessage());
    }

    @Test
    void shouldFailOnSpecialCharactersInGqlStatus() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> new GqlStatus("12_AB"), hint);
        assertEquals(errorMessageStart + "got: 12_AB.", e.getMessage());
    }

    @Test
    void shouldAcceptValidGqlStatus() {
        GqlStatus gqlStatus = assertDoesNotThrow(() -> new GqlStatus("01N12"));
        assertEquals("01N12", gqlStatus.gqlStatusString());
    }

    @Test
    void shouldAcceptLowercaseGqlStatusAndSaveItAsUppercase() {
        GqlStatus gqlStatus = assertDoesNotThrow(() -> new GqlStatus("abcde"));
        assertEquals("ABCDE", gqlStatus.gqlStatusString());
    }

    @Test
    void gqlStatusWithDifferentCaseShouldBeEqual() {
        GqlStatus gqlStatusLower = new GqlStatus("abcde");
        GqlStatus gqlStatusUpper = new GqlStatus("ABCDE");

        assertEquals(gqlStatusLower, gqlStatusUpper);
    }

    private final String hint = "Expected GqlStatus() to throw an IllegalArgumentException, but it didn't.";
    private final String errorMessageStart = "GQLSTATUS must be 5 characters and alphanumeric, ";
}
