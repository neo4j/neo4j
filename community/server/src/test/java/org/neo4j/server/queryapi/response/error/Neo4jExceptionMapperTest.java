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

import java.util.stream.Stream;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.FatalDiscoveryException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransientException;

class Neo4jExceptionMapperTest {

    @ParameterizedTest
    @MethodSource(value = "fixtures")
    void toResponse(Neo4jException neo4jException, Response.Status expectedStatus) {
        try (var expectedResponse = Response.status(expectedStatus)
                .entity(HttpErrorResponse.fromDriverException(neo4jException))
                .build()) {

            var mapper = new Neo4jExceptionMapper();

            try (var response = mapper.toResponse(neo4jException)) {
                Assertions.assertEquals(expectedResponse.getEntity(), response.getEntity());
                Assertions.assertEquals(expectedResponse.getStatus(), response.getStatus());
            }
        }
    }

    public static Stream<Arguments> fixtures() {
        return Stream.of(
                fatalDiscoveryExceptionFixture(),
                clientExceptionFixture(),
                transientExceptionFixture(),
                databaseExceptionFixture(),
                genericNeo4jExceptionFixture());
    }

    private static Arguments fatalDiscoveryExceptionFixture() {
        var fatalDiscoveryException = new FatalDiscoveryException("some test exception");

        return Arguments.of(fatalDiscoveryException, Response.Status.NOT_FOUND);
    }

    private static Arguments clientExceptionFixture() {
        var clientException = new ClientException("some test exception");

        return Arguments.of(clientException, Response.Status.BAD_REQUEST);
    }

    private static Arguments transientExceptionFixture() {
        var transientException = new TransientException("the code", "some test exception");

        return Arguments.of(transientException, Response.Status.BAD_REQUEST);
    }

    private static Arguments databaseExceptionFixture() {
        var databaseException = new DatabaseException("some code", "some test exception");

        return Arguments.of(databaseException, Response.Status.INTERNAL_SERVER_ERROR);
    }

    private static Arguments genericNeo4jExceptionFixture() {
        var neo4jException = Mockito.mock(Neo4jException.class);
        Mockito.when(neo4jException.getMessage()).thenReturn("some test exception");
        Mockito.when(neo4jException.code()).thenReturn("the code");
        Mockito.when(neo4jException.toString()).thenReturn("Neo4jException");

        return Arguments.of(neo4jException, Response.Status.INTERNAL_SERVER_ERROR);
    }
}
