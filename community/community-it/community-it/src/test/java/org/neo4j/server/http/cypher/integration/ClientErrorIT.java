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
package org.neo4j.server.http.cypher.integration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.containsNoErrors;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.hasErrors;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class ClientErrorIT extends AbstractRestFunctionalTestBase
{
    private static final int UNIQUE_ISBN = 12345;

    private static Stream<Arguments> argumentsProvider()
    {
        return Stream.of(
                Arguments.of(
                        "Not a valid query",
                        Status.Statement.SyntaxError
                ),
                Arguments.of(
                        "RETURN $foo",
                        Status.Statement.ParameterMissing
                ),
                Arguments.of(
                        "MATCH (n) WITH n.prop AS n2 RETURN n2.prop",
                        Status.Statement.TypeError
                ),
                Arguments.of(
                        "CYPHER 1.9 EXPLAIN MATCH n RETURN n",
                        Status.Statement.SyntaxError
                ),
                Arguments.of(
                        "RETURN 10 / 0",
                        Status.Statement.ArithmeticError
                ),
                Arguments.of(
                        "SHOW DATABASES",
                        Status.Statement.NotSystemDatabaseError
                ),
                Arguments.of(
                        "CREATE INDEX FOR (n:Person) ON (n.name)",
                        Status.Transaction.ForbiddenDueToTransactionType
                ),
                Arguments.of(
                        "CREATE (n:``)",
                        Status.Statement.SyntaxError
                ),
                Arguments.of(
                        "CREATE (b:Book {isbn: " + UNIQUE_ISBN + "})",
                        Status.Schema.ConstraintValidationFailed
                ),
                Arguments.of(
                        "LOAD CSV FROM 'http://127.0.0.1/null/' AS line CREATE (a {name:line[0]})", // invalid for json
                        Status.Request.InvalidFormat
                )
        );
    }

    @BeforeAll
    public static void prepareDatabase()
    {
        POST( txCommitUri(), quotedJson(
                "{'statements': [{'statement': 'CREATE INDEX FOR (n:Book) ON (n.name)'}]}" ) );

        POST( txCommitUri(), quotedJson(
                "{'statements': [{'statement': 'CREATE CONSTRAINT ON (b:Book) ASSERT b.isbn IS UNIQUE'}]}" ) );

        POST( txCommitUri(), quotedJson(
                "{'statements': [{'statement': 'CREATE (b:Book {isbn: " + UNIQUE_ISBN + "})'}]}" ) );
    }

    @ParameterizedTest( name = "{0} should cause {1}" )
    @MethodSource( "argumentsProvider" )
    public void clientErrorShouldRollbackTheTransaction( String query, Status errorStatus ) throws JsonParseException
    {
        // Given
        HTTP.Response first = POST( txUri(), quotedJson( "{'statements': [{'statement': 'CREATE (n {prop : 1})'}]}" ) );
        assertThat( first.status() ).isEqualTo( 201 );
        assertThat( first ).satisfies( containsNoErrors() );
        long txId = extractTxId( first );

        // When
        HTTP.Response malformed = POST( txUri( txId ),
                quotedJson( "{'statements': [{'statement': '" + query + "'}]}" ) );

        // Then

        // malformed POST contains expected error
        assertThat( malformed.status() ).isEqualTo( 200 );
        assertThat( malformed ).satisfies( hasErrors( errorStatus ) );

        // transaction was rolled back on the previous step and we can't commit it
        HTTP.Response commit = POST( first.stringFromContent( "commit" ) );
        assertThat( commit.status() ).isEqualTo( 404 );
    }
}
