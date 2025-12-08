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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.queryapi.exception.QueryApiException;

class HttpErrorResponseTest {

    @Test
    void fromDriverException() {
        var expectedMessage = "The message";
        var expectedCode = "the code";
        var expectedHttpResponse = new HttpErrorResponse(List.of(new HttpError(expectedCode, expectedMessage)));

        var ex = Mockito.mock(Neo4jException.class);
        Mockito.when(ex.getMessage()).thenReturn(expectedMessage);
        Mockito.when(ex.code()).thenReturn(expectedCode);

        var response = HttpErrorResponse.fromDriverException(ex);

        Assertions.assertEquals(expectedHttpResponse, response);
    }

    @Test
    void fromQueryApiException() {
        var status = Status.Request.Invalid;
        var expectedMessage = "The message";
        var expectedCode = status.code().serialize();
        var expectedHttpResponse = new HttpErrorResponse(List.of(new HttpError(expectedCode, expectedMessage)));

        var ex = Mockito.mock(QueryApiException.class);
        Mockito.when(ex.getMessage()).thenReturn(expectedMessage);
        Mockito.when(ex.status()).thenReturn(status);

        var response = HttpErrorResponse.fromQueryApiException(ex);

        Assertions.assertEquals(expectedHttpResponse, response);
    }
}
