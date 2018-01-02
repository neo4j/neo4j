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

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.neo4j.kernel.api.exceptions.Status.Request.InvalidFormat;
import static org.neo4j.kernel.api.exceptions.Status.Statement.InvalidSyntax;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.containsNoStackTraces;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.hasErrors;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

/**
 * Tests for error messages and graceful handling of problems with the transactional endpoint.
 */
public class TransactionErrorIT extends AbstractRestFunctionalTestBase
{
    @Test
    public void begin__commit_with_invalid_cypher() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        HTTP.Response response = POST( txUri(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' } ] }" ) );
        String commitResource = response.stringFromContent( "commit" );

        // commit with invalid cypher
        response = POST( commitResource, quotedJson( "{ 'statements': [ { 'statement': 'CREATE ;;' } ] }" ) );

        assertThat( response.status(), is( 200 ) );
        assertThat( response, hasErrors( InvalidSyntax ) );
        assertThat( response, containsNoStackTraces());

        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction ) );
    }

    @Test
    public void begin__commit_with_malformed_json() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        HTTP.Response begin = POST( txUri(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE n' } ] }" ) );
        String commitResource = begin.stringFromContent( "commit" );

        // commit with malformed json
        HTTP.Response response = POST( commitResource, rawPayload( "[{asd,::}]" ) );

        assertThat( response.status(), is( 200 ) );
        assertThat( response, hasErrors( InvalidFormat ) );

        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction ) );
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void begin_and_execute_periodic_commit_that_fails() throws Exception
    {
        File file = File.createTempFile("begin_and_execute_periodic_commit_that_fails", ".csv").getAbsoluteFile();
        try {
            PrintStream out = new PrintStream( new FileOutputStream( file ) );
            out.println("1");
            out.println("2");
            out.println("0");
            out.println("3");
            out.close();

            String url = file.toURI().toURL().toString().replace("\\", "\\\\");
            String query = "USING PERIODIC COMMIT 1 LOAD CSV FROM \\\"" + url + "\\\" AS line CREATE ({name: 1/toInt(line[0])});";

            // begin and execute and commit
            HTTP.RawPayload payload = quotedJson("{ 'statements': [ { 'statement': '" + query + "' } ] }");
            HTTP.Response response = POST( txCommitUri(), payload);

            assertThat( response.status(), equalTo( 200 ) );
            assertThat( response, hasErrors(Status.Statement.ArithmeticError) );

            JsonNode message = response.get( "errors" ).get( 0 ).get( "message" );
            assertTrue("Expected LOAD CSV line number information",
                    message
                            .toString()
                            .contains("on line 3. Possibly the last row committed during import is line 2. Note that this information might not be accurate."));
        }
        finally {
            file.delete();
        }
    }

    private long countNodes()
    {
        return TransactionMatchers.countNodes( graphdb() );
    }
}
