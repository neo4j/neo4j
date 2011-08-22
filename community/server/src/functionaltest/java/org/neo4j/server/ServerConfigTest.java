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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.server.ServerBuilder.server;

import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Test;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;

public class ServerConfigTest
{

    private NeoServerWithEmbeddedWebServer server;

    @After
    public void stopTheServer()
    {
        server.stop();
    }

    @Test
    public void shouldPickUpPortFromConfig() throws Exception {
        final int NON_DEFAULT_PORT = 4321;

        server = server().onPort(NON_DEFAULT_PORT)
                .build();
        server.start();

        assertEquals(NON_DEFAULT_PORT, server.getWebServerPort());

        JaxRsResponse response = new RestRequest(server.baseUri()).get();

        assertThat(response.getStatus(), is(200));
        response.close();
    }

    @Test
    public void shouldPickupRelativeUrisForWebAdminAndWebAdminRest() throws IOException
    {
        String webAdminDataUri = "/a/different/webadmin/data/uri/";
        String webAdminManagementUri = "/a/different/webadmin/management/uri/";

        server = server().withRelativeWebDataAdminUriPath( webAdminDataUri )
                .withRelativeWebAdminUriPath( webAdminManagementUri )
                .build();
        server.start();

        JaxRsResponse response = new RestRequest().get("http://localhost:7474" + webAdminDataUri, MediaType.TEXT_HTML_TYPE);
        assertEquals( 200, response.getStatus() );

        response = new RestRequest().get("http://localhost:7474" + webAdminManagementUri);
        assertEquals( 200, response.getStatus() );
        response.close();
    }
}
