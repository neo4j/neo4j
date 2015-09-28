/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.harness.doc;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Function;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.server.HTTP;

import static org.junit.Assert.*;

public class ExtensionTestingDocTest
{
    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    // START SNIPPET: testExtension
    @Path("")
    public static class MyUnmanagedExtension
    {
        @GET
        public Response myEndpoint()
        {
            return Response.ok().build();
        }
    }

    @Test
    public void testMyExtension() throws Exception
    {
        // Given
        try ( ServerControls server = TestServerBuilders.newInProcessBuilder()
                .withExtension( "/myExtension", MyUnmanagedExtension.class )
                .newServer() )
        {
            // When
            HTTP.Response response = HTTP.GET( server.httpURI().resolve( "myExtension" ).toString() );

            // Then
            assertEquals( 200, response.status() );
        }
    }

    @Test
    public void testMyExtensionWithFunctionFixture() throws Exception
    {
        // Given
        try ( ServerControls server = TestServerBuilders.newInProcessBuilder()
                .withExtension( "/myExtension", MyUnmanagedExtension.class )
                .withFixture( new Function<GraphDatabaseService, Void>()
                {
                    @Override
                    public Void apply( GraphDatabaseService graphDatabaseService ) throws RuntimeException
                    {
                        try ( Transaction tx = graphDatabaseService.beginTx() )
                        {
                            graphDatabaseService.createNode( DynamicLabel.label( "User" ) );
                            tx.success();
                        }
                        return null;
                    }
                } )
                .newServer() )
        {
            // When
            Result result = server.graph().execute( "MATCH (n:User) return n" );

            // Then
            assertEquals( 1, IteratorUtil.count( result ) );
        }
    }
    // END SNIPPET: testExtension
}
