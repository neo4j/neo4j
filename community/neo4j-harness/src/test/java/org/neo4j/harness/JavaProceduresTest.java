/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.junit.Test;

import java.util.stream.Stream;

import org.neo4j.kernel.impl.proc.ReadOnlyProcedure;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.server.HTTP;

import static junit.framework.TestCase.assertEquals;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class JavaProceduresTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( InProcessBuilderTest.class );

    @Rule public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    public static class MyProcedures
    {
        public static class OutputRecord
        {
            public long someNumber = 1337;
        }

        @ReadOnlyProcedure
        public Stream<OutputRecord> myProc()
        {
            return Stream.of( new OutputRecord() );
        }

        @ReadOnlyProcedure
        public Stream<OutputRecord> procThatThrows()
        {
            throw new RuntimeException( "This is an exception" );
        }
    }

    @Test
    public void shouldLaunchWithDeclaredProcedures() throws Exception
    {
        // When
        try(ServerControls server = TestServerBuilders.newInProcessBuilder().withProcedure( MyProcedures.class ).newServer())
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/data/transaction/commit" ).toString(),
                    quotedJson( "{ 'statements': [ { 'statement': 'CALL org.neo4j.harness.myProc' } ] }" ) );

            JsonNode result = response.get( "results" ).get( 0 );
            assertEquals( "someNumber", result.get( "columns" ).get( 0 ).asText() );
            assertEquals( 1337, result.get( "data" ).get( 0 ).get( "row" ).get( 0 ).asInt() );
        }
    }

    @Test
    public void shouldGetHelpfulErrorOnProcedureThrowsException() throws Exception
    {
        // When
        try(ServerControls server = TestServerBuilders.newInProcessBuilder().withProcedure( MyProcedures.class ).newServer())
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/data/transaction/commit" ).toString(),
                    quotedJson( "{ 'statements': [ { 'statement': 'CALL org.neo4j.harness.procThatThrows' } ] }" ) );

            String error = response.get( "errors" ).get( 0 ).get( "message" ).asText();
            assertEquals( "Failed to invoke procedure `org.neo4j.harness.procThatThrows() :: (someNumber :: INTEGER?)`: This is an exception", error );
        }
    }
}
