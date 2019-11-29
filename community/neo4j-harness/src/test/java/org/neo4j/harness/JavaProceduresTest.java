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

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.stream.Stream;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class JavaProceduresTest
{
    @Inject
    private TestDirectory testDir;

    public static class MyProcedures
    {
        public static class OutputRecord
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

    public static class MyProceduresUsingMyService
    {
        public static class OutputRecord
        {
            public String result;
        }

        @Context
        public SomeService service;

        @Procedure( "hello" )
        public Stream<OutputRecord> hello()
        {
            OutputRecord t = new OutputRecord();
            t.result = service.hello();
            return Stream.of( t );
        }
    }

    public static class MyProceduresUsingMyCoreAPI
    {
        public static class LongResult
        {
            public Long value;
        }

        @Context
        public MyCoreAPI myCoreAPI;

        @Context
        public Transaction transaction;

        @Procedure( value = "makeNode", mode = Mode.WRITE )
        public Stream<LongResult> makeNode( @Name( "label" ) String label ) throws ProcedureException
        {
            LongResult t = new LongResult();
            t.value = myCoreAPI.makeNode( transaction, label );
            return Stream.of( t );
        }

        @Procedure( value = "willFail", mode = Mode.READ )
        public Stream<LongResult> willFail() throws ProcedureException
        {
            LongResult t = new LongResult();
            t.value = myCoreAPI.makeNode( transaction, "Test" );
            return Stream.of( t );
        }

        @Procedure( "countNodes" )
        public Stream<LongResult> countNodes()
        {
            LongResult t = new LongResult();
            t.value = myCoreAPI.countNodes( transaction );
            return Stream.of( t );
        }
    }

    private Neo4jBuilder createServer( Class<?> procedureClass )
    {
        return Neo4jBuilders.newInProcessBuilder()
                                 .withProcedure( procedureClass );
    }

    @Test
    void shouldLaunchWithDeclaredProcedures() throws Exception
    {
        // When
        try ( Neo4j server = createServer( MyProcedures.class ).build() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/neo4j/tx/commit" ).toString(),
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
        try ( Neo4j server = createServer( MyProcedures.class ).build() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/neo4j/tx/commit" ).toString(),
                    quotedJson( "{ 'statements': [ { 'statement': 'CALL org.neo4j.harness.procThatThrows' } ] }" ) );

            String error = response.get( "errors" ).get( 0 ).get( "message" ).asText();
            assertEquals( "Failed to invoke procedure `org.neo4j.harness.procThatThrows`: " +
                    "Caused by: java.lang.RuntimeException: This is an exception", error );
        }
    }

    @Test
    void shouldWorkWithInjectableFromExtension() throws Throwable
    {
        // When
        try ( Neo4j server = createServer( MyProceduresUsingMyService.class ).build() )
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/neo4j/tx/commit" ).toString(),
                    quotedJson( "{ 'statements': [ { 'statement': 'CALL hello' } ] }" ) );

            assertEquals( "[]", response.get( "errors" ).toString() );
            JsonNode result = response.get( "results" ).get( 0 );
            assertEquals( "result", result.get( "columns" ).get( 0 ).asText() );
            assertEquals( "world", result.get( "data" ).get( 0 ).get( "row" ).get( 0 ).asText() );
        }
    }

    @Test
    void shouldWorkWithInjectableFromExtensionWithMorePower() throws Throwable
    {
        // When
        try ( Neo4j server = createServer( MyProceduresUsingMyCoreAPI.class ).build() )
        {
            // Then
            assertQueryGetsValue( server, "CALL makeNode(\\'Test\\')", 0L );
            assertQueryGetsValue( server, "CALL makeNode(\\'Test\\')", 1L );
            assertQueryGetsValue( server, "CALL makeNode(\\'Test\\')", 2L );
            assertQueryGetsValue( server, "CALL countNodes", 3L );
            assertQueryGetsError( server, "CALL willFail", "Write operations are not allowed" );
        }
    }

    private void assertQueryGetsValue( Neo4j server, String query, long value ) throws Throwable
    {
        HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/neo4j/tx/commit" ).toString(),
                quotedJson( "{ 'statements': [ { 'statement': '" + query + "' } ] }" ) );

        assertEquals( "[]", response.get( "errors" ).toString() );
        JsonNode result = response.get( "results" ).get( 0 );
        assertEquals( "value", result.get( "columns" ).get( 0 ).asText() );
        assertEquals( value, result.get( "data" ).get( 0 ).get( "row" ).get( 0 ).asLong() );
    }

    private void assertQueryGetsError( Neo4j server, String query, String error ) throws Throwable
    {
        HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/neo4j/tx/commit" ).toString(),
                quotedJson( "{ 'statements': [ { 'statement': '" + query + "' } ] }" ) );

        assertThat( response.get( "errors" ).toString() ).contains( error );
    }
}
