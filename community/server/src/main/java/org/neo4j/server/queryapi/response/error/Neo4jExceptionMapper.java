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

import static org.neo4j.server.queryapi.response.error.HttpErrorResponse.fromDriverException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.FatalDiscoveryException;
import org.neo4j.driver.exceptions.GqlStatusErrorClassification;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransientException;

public class Neo4jExceptionMapper implements ExceptionMapper<Neo4jException> {

    @Override
    public Response toResponse(Neo4jException e) {
        return Response.status(status(e)).entity(fromDriverException(e)).build();
    }

    private static Response.Status status(Neo4jException e) {
        if (e instanceof FatalDiscoveryException) {
            return Response.Status.NOT_FOUND;
        } else if (e instanceof ClientException || e instanceof TransientException) {
            return Response.Status.BAD_REQUEST;
        } else if (e instanceof DatabaseException) {
            // use GQL Classification where possible.
            if (e.classification()
                    .filter(c -> c.equals(GqlStatusErrorClassification.CLIENT_ERROR))
                    .isPresent()) {
                return Response.Status.BAD_REQUEST;
            }
        }
        // Database error and unclassified
        return Response.Status.INTERNAL_SERVER_ERROR;
    }
}
