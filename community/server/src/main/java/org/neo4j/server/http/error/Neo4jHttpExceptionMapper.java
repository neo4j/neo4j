/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.http.error;

import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class Neo4jHttpExceptionMapper implements ExceptionMapper<Neo4jHttpException>
{
    @Override
    public Response toResponse( Neo4jHttpException exception )
    {
        List<ErrorRepresentation.Error> errors = exception.getNeo4jErrors().stream()
                .map( e -> new ErrorRepresentation.Error( e.status().code().serialize(), e.getMessage() ) )
                .collect( Collectors.toList() );

        ErrorRepresentation errorEntity = new ErrorRepresentation();
        errorEntity.setErrors( errors );

        return Response.status( exception.getHttpStatus() ).type( MediaType.APPLICATION_JSON_TYPE ).entity( errorEntity ).build();
    }
}
