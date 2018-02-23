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
package org.neo4j.harness;

import org.codehaus.jackson.JsonNode;
import org.junit.Rule;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class JavaProceduresTest
{
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    static class MyProcedures
    {
        static class OutputRecord
        {
            public long someNumber = 1337;
        }

        @Procedure
        public Stream<OutputRecord> myProc()
        {
            return Stream.of( new OutputRecord() );
        }

        @Procedure
        public Stream<OutputRecord> procThatThrows()
        {
            throw new RuntimeException( "This is an exception" );
        }
    }

    static class MyProceduresUsingMyService
    {
        static class OutputRecord
        {
            String result;
        }

        @Context
        SomeService service;

        @Procedure( "hello" )
        public Stream<OutputRecord> hello()
        {
            OutputRecord t = new OutputRecord();
            t.result = service.hello();
            return Stream.of( t );
        }
    }

    static class MyProceduresUsingMyCoreAPI
    {
        static class LongResult
        {
            Long value;
        }

        @Context
        MyCoreAPI myCoreAPI;

        @Procedure( value = "makeNode", mode = Mode.WRITE )
        public Stream<LongResult> makeNode( @Name( "label" ) String label ) throws ProcedureException
        {
            LongResult t = new LongResult();
            t.value = myCoreAPI.makeNode( label );
            return Stream.of( t );
        }

        @Procedure( value = "willFail", mode = Mode.READ )
        public Stream<LongResult> willFail() throws ProcedureException
        {
            LongResult t = new LongResult();
            t.value = myCoreAPI.makeNode( "Test" );
            return Stream.of( t );
        }

        @Procedure( "countNodes" )
        public Stream<LongResult> countNodes()
        {
            LongResult t = new LongResult();
            t.value = myCoreAPI.countNodes();
            return Stream.of( t );
        }
    }

    private TestServerBuilder createServer( Class<?> procedureClass )
    {
        return TestServerBuilders.newInProcessBuilder()
                                 .withProcedure( procedureClass )
                                 .withConfig( ServerSettings.script_enabled, Settings.TRUE );
    }

    @Test
    void shouldLaunchWithDeclaredProcedures() throws Exception
    {
        // When
        try ( ServerControls server = createServer( MyProcedures.class ).newServer() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/data/transaction/commit" ).toString(),
                    quotedJson( "{ 'statements': [ { 'statement': 'CALL org.neo4j.harness.myProc' } ] }" ) );

            JsonNode result = response.get( "results" ).get( 0 );
            assertEquals( "someNumber", result.get( "columns" ).get( 0 ).asText() );
            assertEquals( 1337, result.get( "data" ).get( 0 ).get( "row" ).get( 0 ).asInt() );
            assertEquals( "[]", response.get( "errors" ).toString() );
        }
    }

    @Test
    void shouldGetHelpfulErrorOnProcedureThrowsException() throws Exception
    {
        // When
        try ( ServerControls server = createServer( MyProcedures.class ).newServer() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/data/transaction/commit" ).toString(),
                    quotedJson( "{ 'statements': [ { 'statement': 'CALL org.neo4j.harness.procThatThrows' } ] }" ) );

            String error = response.get( "errors" ).get( 0 ).get( "message" ).asText();
            assertEquals( "Failed to invoke procedure `org.neo4j.harness.procThatThrows`: " +
                    "Caused by: java.lang.RuntimeException: This is an exception", error );
        }
    }

    @Test
    void shouldWorkWithInjectableFromKernelExtension() throws Throwable
    {
        // When
        try ( ServerControls server = createServer( MyProceduresUsingMyService.class ).newServer() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/data/transaction/commit" ).toString(),
                    quotedJson( "{ 'statements': [ { 'statement': 'CALL hello' } ] }" ) );

            assertEquals( "[]", response.get( "errors" ).toString() );
            JsonNode result = response.get( "results" ).get( 0 );
            assertEquals( "result", result.get( "columns" ).get( 0 ).asText() );
            assertEquals( "world", result.get( "data" ).get( 0 ).get( "row" ).get( 0 ).asText() );
        }
    }

    @Test
    void shouldWorkWithInjectableFromKernelExtensionWithMorePower() throws Throwable
    {
        // When
        try ( ServerControls server = createServer( MyProceduresUsingMyCoreAPI.class )
                .withConfig( GraphDatabaseSettings.record_id_batch_size, "1" )
                .newServer() )
        {
            // Then
            assertQueryGetsValue( server, "CALL makeNode(\\'Test\\')", 0L );
            assertQueryGetsValue( server, "CALL makeNode(\\'Test\\')", 1L );
            assertQueryGetsValue( server, "CALL makeNode(\\'Test\\')", 2L );
            assertQueryGetsValue( server, "CALL countNodes", 3L );
            assertQueryGetsError( server, "CALL willFail", "Write operations are not allowed" );
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
