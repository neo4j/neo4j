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
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.test.Mute;
import org.neo4j.test.server.HTTP;

import static org.junit.Assert.*;

public class ExtensionTestingDocTest
{
    @Rule public Mute mute = Mute.muteAll();

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
    // END SNIPPET: testExtension
}
