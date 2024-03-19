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
package org.neo4j.server.http.error;

import static org.neo4j.server.httpv2.response.HttpErrorResponse.singleError;

import com.fasterxml.jackson.core.JacksonException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.neo4j.kernel.api.exceptions.Status;

public class JacksonExceptionMapper implements ExceptionMapper<JacksonException> {

    @Override
    public Response toResponse(JacksonException exception) {
        // For some reason, json parsing errors return a 500 status code.
        // todo make this error more helpful.
        return Response.status(400)
                .entity(singleError(Status.Request.Invalid.code().serialize(), "Request body invalid."))
                .build();
    }
}
