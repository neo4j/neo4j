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
import java.util.stream.Stream;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.server.queryapi.exception.QueryApiException;
import org.neo4j.server.queryapi.exception.UnknownTypeException;

class QueryApiExceptionMapperTest {

    @ParameterizedTest
    @MethodSource(value = "fixtures")
    void toResponse(QueryApiException queryApiException, Response.Status expectedStatus) {
        try (var expectedResponse = Response.status(expectedStatus)
                .entity(HttpErrorResponse.fromQueryApiException(queryApiException))
                .build()) {

            var mapper = new QueryApiExceptionMapper();

            try (var response = mapper.toResponse(queryApiException)) {
                Assertions.assertEquals(expectedResponse.getEntity(), response.getEntity());
                Assertions.assertEquals(expectedResponse.getStatus(), response.getStatus());
            }
        }
    }

    public static Stream<Arguments> fixtures() {
        return Stream.of(unknownTypeExceptionFixture());
    }

    private static Arguments unknownTypeExceptionFixture() {
        var unknownTypeException = new UnknownTypeException("[1, 2, 3]", List.of("String", "Number"), "VECTOR");
        return Arguments.of(unknownTypeException, Response.Status.BAD_REQUEST);
    }
}
