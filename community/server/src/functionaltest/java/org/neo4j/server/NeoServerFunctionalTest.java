/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.FunctionalTestHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;

public class NeoServerFunctionalTest
{

    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerHelper.createServer();
        functionalTestHelper = new FunctionalTestHelper(server);
    }

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
    }

    @AfterClass
    public static void stopServer()
    {
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void whenServerIsStartedItshouldStartASingleDatabase() throws Exception
    {
        assertNotNull( server.getDatabase() );
    }

    @Test
    public void shouldRedirectRootToWebadmin() throws Exception {
        assertFalse(server.baseUri()
                .toString()
                .contains("webadmin"));
        JaxRsResponse response = RestRequest.req().get(server.baseUri().toString(), MediaType.TEXT_HTML_TYPE);
        assertThat(response.getStatus(), is(200));
        assertThat(response.getEntity(), containsString("webadmin"));
    }

    @Test
    public void serverShouldProvideAWelcomePage() throws Exception {
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.getWebadminUri());

        assertThat(response.getStatus(), is(200));
        assertThat(response.getHeaders().getFirst("Content-Type"), containsString("html"));
    }

}
