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
package org.neo4j.server.queryapi.response.error;

import java.util.List;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.exceptions.Status;

class InternalServerExceptionMapperTest {

    @Test
    void toResponse() {
        try (var expectedResponse = Response.serverError()
                .entity(new HttpErrorResponse(List.of(new HttpError(
                        Status.General.UnknownError.code().serialize(),
                        Status.General.UnknownError.code().description()))))
                .build()) {

            var ex = new Exception("Fun ex");
            var mapper = new InternalServerExceptionMapper();

            try (var response = mapper.toResponse(ex)) {
                Assertions.assertEquals(expectedResponse.getEntity(), response.getEntity());
                Assertions.assertEquals(expectedResponse.getStatus(), response.getStatus());
            }
        }
    }
}
