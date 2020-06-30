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

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.ParameterizedTransactionEndpointsTestBase;
import org.neo4j.test.server.HTTP;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.api.exceptions.Status.Request.InvalidFormat;
import static org.neo4j.kernel.api.exceptions.Status.Statement.SyntaxError;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.containsNoStackTraces;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.hasErrors;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

/**
 * Tests for error messages and graceful handling of problems with the transactional endpoint.
 */
public class TransactionErrorIT extends ParameterizedTransactionEndpointsTestBase
{
    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin__commit_with_invalid_cypher( String txUri ) throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        HTTP.Response response = POST( txUri, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        String commitResource = response.stringFromContent( "commit" );

        // commit with invalid cypher
        response = POST( commitResource, quotedJson( "{ 'statements': [ { 'statement': 'CREATE ;;' } ] }" ) );

        assertThat( response.status() ).isEqualTo( 200 );
        assertThat( response ).satisfies( hasErrors( SyntaxError ) );
        assertThat( response ).satisfies( containsNoStackTraces() );

        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin__commit_with_malformed_json( String txUri ) throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        HTTP.Response begin = POST( txUri, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        String commitResource = begin.stringFromContent( "commit" );

        // commit with malformed json
        HTTP.Response response = POST( commitResource, rawPayload( "[{asd,::}]" ) );

        assertThat( response.status() ).isEqualTo( 200 );
        assertThat( response ).satisfies( hasErrors( InvalidFormat ) );

        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    @SuppressWarnings( "ResultOfMethodCallIgnored" )
    public void begin_and_execute_periodic_commit_that_fails( String txUri ) throws Exception
    {
        File file = File.createTempFile("begin_and_execute_periodic_commit_that_fails", ".csv").getAbsoluteFile();
        try
        {
            PrintStream out = new PrintStream( new FileOutputStream( file ) );
            out.println("1");
            out.println("2");
            out.println("0");
            out.println("3");
            out.close();

            String url = file.toURI().toURL().toString().replace("\\", "\\\\");
            String query = "USING PERIODIC COMMIT 1 LOAD CSV FROM \\\"" + url + "\\\" AS line CREATE ({name: 1/toInteger(line[0])});";

            // begin and execute and commit
            HTTP.RawPayload payload = quotedJson("{ 'statements': [ { 'statement': '" + query + "' } ] }");
            HTTP.Response response = POST( txUri + "/commit", payload );

            assertThat( response.status() ).isEqualTo( 200 );
            assertThat( response ).satisfies( hasErrors( Status.Statement.ArithmeticError ) );

            JsonNode message = response.get( "errors" ).get( 0 ).get( "message" );
            assertTrue( message.toString().contains("on line 3."), "Expected LOAD CSV line number information" );
        }
        finally
        {
            file.delete();
        }
    }

    private long countNodes()
    {
        return TransactionConditions.countNodes( graphdb() );
    }
}
