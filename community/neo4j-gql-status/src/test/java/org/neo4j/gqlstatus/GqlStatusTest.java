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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class GqlStatusTest {
    @Test
    void shouldFormatParams() {
        var statusCode = GqlStatusInfoCodes.STATUS_52N01;
        List<String> paramList = new ArrayList<String>();
        paramList.add("param1");
        paramList.add("param2");
        paramList.add("param3");
        String message = statusCode.getMessage(paramList.toArray());
        assertThat(message).isEqualTo("Execution of the procedure param1() timed out after param2 param3.");
    }

    @Test
    void shouldFailOnEmptyGqlStatus() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new GqlStatus(""))
                .withMessage(errorMessageStart + "got an empty string.");
    }

    @Test
    void shouldFailOnTooShortGqlStatus() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new GqlStatus("125A"))
                .withMessage(errorMessageStart + "got: 125A.");
    }

    @Test
    void shouldFailOnTooLongGqlStatus() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new GqlStatus("125ABC"))
                .withMessage(errorMessageStart + "got: 125ABC.");
    }

    @Test
    void shouldFailOnSpecialCharactersInGqlStatus() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new GqlStatus("12_AB"))
                .withMessage(errorMessageStart + "got: 12_AB.");
    }

    @Test
    void shouldAcceptValidGqlStatus() {
        var gqlStatus = new GqlStatus("01N12");
        assertThat(gqlStatus.gqlStatusString()).isEqualTo("01N12");
    }

    @Test
    void shouldAcceptLowercaseGqlStatusAndSaveItAsUppercase() {
        var gqlStatus = new GqlStatus("abcde");
        assertThat(gqlStatus.gqlStatusString()).isEqualTo("ABCDE");
    }

    @Test
    void gqlStatusWithDifferentCaseShouldBeEqual() {
        GqlStatus gqlStatusLower = new GqlStatus("abcde");
        GqlStatus gqlStatusUpper = new GqlStatus("ABCDE");

        assertThat(gqlStatusLower).isEqualTo(gqlStatusUpper);
    }

    private final String errorMessageStart = "GQLSTATUS must be 5 characters and alphanumeric, ";
}
