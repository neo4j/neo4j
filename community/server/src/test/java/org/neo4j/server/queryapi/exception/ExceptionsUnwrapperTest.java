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
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.driver.exceptions.ClientException;

class ExceptionsUnwrapperTest {

    @ParameterizedTest
    @MethodSource("thrownFixtures")
    void shouldUnwrapAndThrowNeo4jAndQueryApiExceptionsThrownWhen(Exception exception, Exception expectedThrown) {
        var thrown = Assertions.assertThrows(
                expectedThrown.getClass(),
                () -> ExceptionsUnwrapper.unwrapAndThrowNeo4jAndQueryApiExceptions(exception));

        Assertions.assertEquals(expectedThrown, thrown);
    }

    @ParameterizedTest
    @MethodSource("notThrownFixtures")
    void shouldNotThrowNeo4jAndQueryApiExceptionsThrownWhen(Exception exception) {
        Assertions.assertDoesNotThrow(() -> ExceptionsUnwrapper.unwrapAndThrowNeo4jAndQueryApiExceptions(exception));
    }

    private static Stream<Arguments> thrownFixtures() {
        return Stream.of(
                neo4jExceptionFixture(),
                neo4jExceptionFixture(1),
                neo4jExceptionFixture(5),
                queryApiExceptionFixture(),
                queryApiExceptionFixture(2),
                queryApiExceptionFixture(10));
    }

    private static Stream<Arguments> notThrownFixtures() {
        return Stream.of(
                genericExceptionFixture(),
                genericExceptionFixture(1),
                genericExceptionFixture(2),
                genericExceptionFixture(5),
                genericExceptionFixture(10));
    }

    private static Arguments neo4jExceptionFixture() {
        return neo4jExceptionFixture(0);
    }

    private static Arguments neo4jExceptionFixture(int wrappedTimes) {
        var clientException = new ClientException("code", "message");
        var exception = (Exception) clientException;

        for (int i = 0; i < wrappedTimes; i++) {
            exception = new Exception(String.format("wrapper %d", i), exception);
        }
        return Arguments.of(exception, clientException);
    }

    private static Arguments queryApiExceptionFixture() {
        return queryApiExceptionFixture(0);
    }

    private static Arguments queryApiExceptionFixture(int wrappedTimes) {
        var queryApiFixture = new UnknownTypeException("[1, 2]", List.of("String"), "Vector");
        var exception = (Exception) queryApiFixture;

        for (int i = 0; i < wrappedTimes; i++) {
            exception = new Exception(String.format("wrapper %d", i), exception);
        }
        return Arguments.of(exception, queryApiFixture);
    }

    private static Arguments genericExceptionFixture() {
        return genericExceptionFixture(0);
    }

    private static Arguments genericExceptionFixture(int wrappedTimes) {
        var exception = new Exception("deeper exception");

        for (int i = 0; i < wrappedTimes; i++) {
            exception = new Exception(String.format("wrapper %d", i), exception);
        }
        return Arguments.of(exception);
    }
}
