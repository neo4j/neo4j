/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
