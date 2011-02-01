/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBuilder;

import javax.ws.rs.core.MediaType;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class RedirectorTests
{
    private NeoServer server;

    @Before
    public void setupServer() throws IOException
    {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
    }

    @After
    public void stopServer()
    {
        server.stop();
    }

    @Test
    public void shouldRedirectRootToWebadmin() throws Exception
    {
        ClientResponse response = Client.
                create().
                resource( server.baseUri() ).
                type( MediaType.APPLICATION_JSON ).
                accept( MediaType.APPLICATION_JSON ).
                get( ClientResponse.class );

        assertThat( response.getStatus(), is( not( 404 ) ) );
    }

    @Test
    public void shouldNotRedirectTheRestOfTheWorld() throws Exception
    {
        String url = server.baseUri() + "a/different/relative/webadmin/data/uri/";

        ClientResponse response = Client.
                create().
                resource( url ).
                type( MediaType.APPLICATION_JSON ).
                accept( MediaType.APPLICATION_JSON ).
                get( ClientResponse.class );

        assertThat( response.getStatus(), is( 404 ) );
    }
}
