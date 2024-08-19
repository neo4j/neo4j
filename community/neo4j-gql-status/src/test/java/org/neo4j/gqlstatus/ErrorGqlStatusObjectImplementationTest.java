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

import org.junit.jupiter.api.Test;

class ErrorGqlStatusObjectImplementationTest {
    @Test
    void shouldHandleErrorWithDuplicatedParameter() {
        var errorBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N02);
        errorBuilder.withParam(GqlMessageParams.dbName, "my_db"); // this parameter occurs twice in the message
        errorBuilder.withParam(GqlMessageParams.routingEnabledSetting, "my_setting");
        var error = errorBuilder.build();

        assertThat(error.statusDescription())
                .isEqualTo(
                        "error: connection exception - unable to route to database. Unable to connect to database `my_db`. Server-side routing is disabled. Either connect to `my_db` directly, or enable server-side routing by setting `my_setting=true`.");
    }

    @Test
    void shouldNotFailOnErrorWithTooFewParameters() {
        var errorBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02);
        var error = errorBuilder.build();

        assertThat(error.statusDescription())
                .isEqualTo(
                        "error: procedure exception - procedure execution error. Execution of the procedure `$proc` failed.");
    }

    @Test
    void shouldNotFailOnErrorWithTooManyParameters() {
        var errorBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02);
        errorBuilder.withParam(GqlMessageParams.key, "bar");
        errorBuilder.withParam(GqlMessageParams.proc, "my_proc");
        var error = errorBuilder.build();

        assertThat(error.statusDescription())
                .isEqualTo(
                        "error: procedure exception - procedure execution error. Execution of the procedure `my_proc` failed.");
    }

    @Test
    void shouldNotFailOnErrorWithWrongParameter() {
        var errorBuilder = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02);
        errorBuilder.withParam(GqlMessageParams.key, "bar");
        var error = errorBuilder.build();

        assertThat(error.statusDescription())
                .isEqualTo(
                        "error: procedure exception - procedure execution error. Execution of the procedure `$proc` failed.");
    }
}
