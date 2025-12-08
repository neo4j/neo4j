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
package org.neo4j.server.queryapi.exception;

import java.util.List;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

class UnknownTypeExceptionTest {
    private static final String VALUE = "[1, 2, 3]";
    private static final List<String> SUPPORTED_TYPES = List.of("String", "Integer", "Long", "Double");
    private static final String TYPE = "Vector";

    @Test
    void getMessage() throws Exception {
        var exception = subject();

        Assertions.assertEquals("Type Vector is not supported as a column value", exception.getMessage());
    }

    @Test
    void legacyMessage() throws Exception {
        var exception = subject();

        Assertions.assertEquals("Type Vector is not supported as a column value", exception.legacyMessage());
    }

    @Test
    void status() throws Exception {
        var exception = subject();

        Assertions.assertEquals(Status.Request.Invalid, exception.status());
    }

    @Test
    void httpStatus() throws Exception {
        var exception = subject();

        Assertions.assertEquals(Response.Status.BAD_REQUEST, exception.httpStatus());
    }

    @Test
    void gqlStatusObject() throws Exception {
        var exception = subject();

        Assertions.assertEquals(
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N01)
                                .withParam(GqlParams.StringParam.value, VALUE)
                                .withParam(GqlParams.ListParam.valueTypeList, SUPPORTED_TYPES)
                                .withParam(GqlParams.StringParam.valueType, TYPE)
                                .build())
                        .build(),
                exception.gqlStatusObject());
    }

    private static UnknownTypeException subject() {
        return new UnknownTypeException(VALUE, SUPPORTED_TYPES, TYPE);
    }
}
