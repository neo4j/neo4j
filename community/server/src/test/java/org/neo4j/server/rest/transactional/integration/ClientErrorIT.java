/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.rest.transactional.integration;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.containsNoErrors;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.hasErrors;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

@RunWith( Parameterized.class )
public class ClientErrorIT extends AbstractRestFunctionalTestBase
{
    private static final int UNIQUE_ISBN = 12345;

    @Parameterized.Parameter( 0 )
    public String query;

    @Parameterized.Parameter( 1 )
    public Status errorStatus;

    @Parameterized.Parameters( name = "{0} should cause {1}" )
    public static List<Object[]> queriesWithStatuses()
    {
        return Arrays.asList(
                new Object[]{
                        "Not a valid query",
                        Status.Statement.InvalidSyntax
                },
                new Object[]{
                        "RETURN {foo}",
                        Status.Statement.ParameterMissing
                },
                new Object[]{
                        "MATCH (n) WITH n.prop AS n2 RETURN n2.prop",
                        Status.Statement.InvalidType
                },
                new Object[]{
                        "CYPHER 1.9 EXPLAIN MATCH n RETURN n",
                        Status.Statement.InvalidArguments
                },
                new Object[]{
                        "RETURN 10 / 0",
                        Status.Statement.ArithmeticError
                },
                new Object[]{
                        "CREATE INDEX ON :Person(name)",
                        Status.Transaction.InvalidType
                },
                new Object[]{
                        "CREATE (n:``)",
                        Status.Schema.IllegalTokenName
                },
                new Object[]{
                        "CREATE (b:Book {isbn: " + UNIQUE_ISBN + "})",
                        Status.Schema.ConstraintViolation
                },
                new Object[]{
                        "LOAD CSV FROM 'http://127.0.0.1/null/' AS line CREATE (a {name:line[0]})", // invalid for json
                        Status.Request.InvalidFormat
                }
        );
    }

    @BeforeClass
    public static void prepareDatabase()
    {
        POST( txCommitUri(), quotedJson(
                "{'statements': [{'statement': 'CREATE INDEX ON :Book(name)'}]}" ) );

        POST( txCommitUri(), quotedJson(
                "{'statements': [{'statement': 'CREATE CONSTRAINT ON (b:Book) ASSERT b.isbn IS UNIQUE'}]}" ) );

        POST( txCommitUri(), quotedJson(
                "{'statements': [{'statement': 'CREATE (b:Book {isbn: " + UNIQUE_ISBN + "})'}]}" ) );
    }

    @Test
    public void clientErrorShouldRollbackTheTransaction() throws JsonParseException
    {
        // Given
        HTTP.Response first = POST( txUri(), quotedJson( "{'statements': [{'statement': 'CREATE (n {prop : 1})'}]}" ) );
        assertThat( first.status(), is( 201 ) );
        assertThat( first, containsNoErrors() );
        long txId = extractTxId( first );

        // When
        HTTP.Response malformed = POST( txUri( txId ),
                quotedJson( "{'statements': [{'statement': '" + query + "'}]}" ) );

        // Then

        // malformed POST contains expected error
        assertThat( malformed.status(), is( 200 ) );
        assertThat( malformed, hasErrors( errorStatus ) );

        // transaction was rolled back on the previous step and we can't commit it
        HTTP.Response commit = POST( first.stringFromContent( "commit" ) );
        assertThat( commit.status(), is( 404 ) );
    }
}
