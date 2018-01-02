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
package org.neo4j.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.sun.jersey.api.client.Client;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.test.server.ExclusiveServerTestBase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class NeoServerStartupLoggingDocIT extends ExclusiveServerTestBase
{
    private static ByteArrayOutputStream out;
    private static NeoServer server;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        out = new ByteArrayOutputStream();
        server = ServerHelper.createNonPersistentServer( FormattedLogProvider.toOutputStream( out ) );
    }

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    @Test
    public void shouldLogStartup() throws Exception
    {
        // Check the logs
        assertThat( out.toString().length(), is( greaterThan( 0 ) ) );

        // Check the server is alive
        Client nonRedirectingClient = Client.create();
        nonRedirectingClient.setFollowRedirects( false );
        final JaxRsResponse response = new RestRequest(server.baseUri(), nonRedirectingClient).get();
        assertThat( response.getStatus(), is( greaterThan( 199 ) ) );

    }
}
