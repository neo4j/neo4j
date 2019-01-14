/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.harness;

import org.codehaus.jackson.JsonNode;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.UserFunction;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class JavaFunctionsTestIT
{
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    public static class MyFunctions
    {

        @UserFunction
        public long myFunc()
        {
            return 1337L;
        }

        @UserFunction
        public long funcThatThrows()
        {
            throw new RuntimeException( "This is an exception" );
        }
    }

    public static class MyFunctionsUsingMyService
    {

        @Context
        public SomeService service;

        @UserFunction( "my.hello" )
        public String hello()
        {
            return service.hello();
        }
    }

    public static class MyFunctionsUsingMyCoreAPI
    {
        @Context
        public MyCoreAPI myCoreAPI;

        @UserFunction( value = "my.willFail" )
        public long willFail() throws ProcedureException
        {
            return myCoreAPI.makeNode( "Test" );
        }

        @UserFunction( "my.countNodes" )
        public long countNodes()
        {
            return myCoreAPI.countNodes();
        }
    }

    private TestServerBuilder createServer( Class<?> functionClass )
    {
        return TestServerBuilders.newInProcessBuilder()
                                 .withConfig( ServerSettings.script_enabled, Settings.TRUE )
                                 .withFunction( functionClass );
    }

    @Test
    public void shouldLaunchWithDeclaredFunctions() throws Exception
    {
        // When
        Class<MyFunctions> functionClass = MyFunctions.class;
        try ( ServerControls server = createServer( functionClass ).newServer() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/data/transaction/commit" ).toString(),
                    quotedJson(
                            "{ 'statements': [ { 'statement': 'RETURN org.neo4j.harness.myFunc() AS someNumber' } ] " +
                            "}" ) );

            JsonNode result = response.get( "results" ).get( 0 );
            assertEquals( "someNumber", result.get( "columns" ).get( 0 ).asText() );
            assertEquals( 1337, result.get( "data" ).get( 0 ).get( "row" ).get( 0 ).asInt() );
            assertEquals( "[]", response.get( "errors" ).toString() );
        }
    }

    @Test
    public void shouldGetHelpfulErrorOnProcedureThrowsException() throws Exception
    {
        // When
        try ( ServerControls server = createServer( MyFunctions.class ).newServer() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/data/transaction/commit" ).toString(),
                    quotedJson(
                            "{ 'statements': [ { 'statement': 'RETURN org.neo4j.harness.funcThatThrows()' } ] }" ) );

            String error = response.get( "errors" ).get( 0 ).get( "message" ).asText();
            assertEquals(
                    "Failed to invoke function `org.neo4j.harness.funcThatThrows`: Caused by: java.lang" +
                    ".RuntimeException: This is an exception",
                    error );
        }
    }

    @Test
    public void shouldWorkWithInjectableFromKernelExtension() throws Throwable
    {
        // When
        try ( ServerControls server = createServer( MyFunctionsUsingMyService.class ).newServer() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/data/transaction/commit" ).toString(),
                    quotedJson( "{ 'statements': [ { 'statement': 'RETURN my.hello() AS result' } ] }" ) );

            assertEquals( "[]", response.get( "errors" ).toString() );
            JsonNode result = response.get( "results" ).get( 0 );
            assertEquals( "result", result.get( "columns" ).get( 0 ).asText() );
            assertEquals( "world", result.get( "data" ).get( 0 ).get( "row" ).get( 0 ).asText() );
        }
    }

    @Test
    public void shouldWorkWithInjectableFromKernelExtensionWithMorePower() throws Throwable
    {
        // When
        try ( ServerControls server = createServer( MyFunctionsUsingMyCoreAPI.class ).newServer() )
        {
            HTTP.POST( server.httpURI().resolve( "db/data/transaction/commit" ).toString(),
                    quotedJson( "{ 'statements': [ { 'statement': 'CREATE (), (), ()' } ] }" ) );

            // Then
            assertQueryGetsValue( server, "RETURN my.countNodes() AS value", 3L );
            assertQueryGetsError( server, "RETURN my.willFail() AS value", "Write operations are not allowed" );
        }
    }

    private void assertQueryGetsValue( ServerControls server, String query, long value ) throws Throwable
    {
        HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/data/transaction/commit" ).toString(),
                quotedJson( "{ 'statements': [ { 'statement': '" + query + "' } ] }" ) );

        assertEquals( "[]", response.get( "errors" ).toString() );
        JsonNode result = response.get( "results" ).get( 0 );
        assertEquals( "value", result.get( "columns" ).get( 0 ).asText() );
        assertEquals( value, result.get( "data" ).get( 0 ).get( "row" ).get( 0 ).asLong() );
    }

    private void assertQueryGetsError( ServerControls server, String query, String error ) throws Throwable
    {
        HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/data/transaction/commit" ).toString(),
                quotedJson( "{ 'statements': [ { 'statement': '" + query + "' } ] }" ) );

        assertThat( response.get( "errors" ).toString(), containsString( error ) );
    }
}
