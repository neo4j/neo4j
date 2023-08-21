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

import java.util.Collections;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.glassfish.jersey.message.internal.MessageBodyProviderNotFoundException;

public class MediaTypeExceptionMapper implements ExceptionMapper<InternalServerErrorException> {

    @Override
    public Response toResponse(InternalServerErrorException exception) {
        // Workaround to map errors related to unsupported media types to the correct HTTP status code as
        // org.glassfish.jersey.server.internal.MappableExceptionWrapperInterceptor (Line 44) explicitly maps these
        // exceptions to
        // InternalServerErrorException instead of reporting a client error as outlined in the specification
        var cause = exception.getCause();
        if (cause instanceof MessageBodyProviderNotFoundException) {
            return Response.notAcceptable(Collections.emptyList())
                    .entity(
                            "Unsupported media type - Supported types are application/json and application/vnd.neo4j.jolt")
                    .build();
        }

        // mimic default behavior (error code + empty response body) - error details will be logged as usual prior to
        // invoking this exception mapper
        return Response.serverError().build();
    }
}
