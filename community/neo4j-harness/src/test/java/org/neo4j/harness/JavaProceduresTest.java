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

import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
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

        @Procedure("hello")
        public Stream<OutputRecord> hello()
        {
            OutputRecord t = new OutputRecord();
            t.result = service.hello();
            return Stream.of( t );
        }
    }

    public static class SomeService
    {
        public String hello()
        {
            return "world";
        }
    }

    // This ensures a non-public mechanism for adding new context components
    // to Procedures is testable via Harness. While this is not public API,
    // this is a vital mechanism to cover use cases Procedures need to cover,
    // and is in place as an approach that should either eventually be made
    // public, or the relevant use cases addressed in other ways.
    public static class MyExtensionThatAddsInjectable
            extends KernelExtensionFactory<MyExtensionThatAddsInjectable.Dependencies>
    {
        public MyExtensionThatAddsInjectable()
        {
            super( "my-ext" );
        }

        @Override
        public Lifecycle newInstance( KernelContext context,
                Dependencies dependencies ) throws Throwable
        {
            dependencies.procedures().registerComponent( SomeService.class, (ctx) -> new SomeService() );
            return new LifecycleAdapter();
        }

        public interface Dependencies
        {
            Procedures procedures();
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
            assertEquals( "[]", response.get( "errors" ).toString() );
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
            assertEquals( "Failed to invoke procedure `org.neo4j.harness.procThatThrows`: Caused by: java.lang.RuntimeException: This is an exception", error );
        }
    }

    @Test
    public void shouldWorkWithInjectableFromKernelExtension() throws Throwable
    {
        // When
        try(ServerControls server = TestServerBuilders.newInProcessBuilder().withProcedure( MyProceduresUsingMyService.class ).newServer())
        {
            // Then
            HTTP.Response response = HTTP.POST( server.httpURI().resolve( "db/data/transaction/commit" ).toString(),
                    quotedJson( "{ 'statements': [ { 'statement': 'CALL hello' } ] }" ) );

            JsonNode result = response.get( "results" ).get( 0 );
            assertEquals( "result", result.get( "columns" ).get( 0 ).asText() );
            assertEquals( "world", result.get( "data" ).get( 0 ).get( "row" ).get( 0 ).asText() );
            assertEquals( "[]", response.get( "errors" ).toString() );
        }
    }
}
