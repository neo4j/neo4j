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
package org.neo4j.server;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.server.helpers.WebContainerHelper;
import org.neo4j.test.server.ExclusiveWebContainerTestBase;

import static java.net.http.HttpClient.Redirect.NEVER;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.LOCATION;
import static javax.ws.rs.core.MediaType.TEXT_HTML;
import static org.junit.Assert.assertEquals;

public class RedirectToBrowserTestIT extends ExclusiveWebContainerTestBase
{
    private static TestWebContainer webContainer;

    @BeforeClass
    public static void startServer() throws Exception
    {
        webContainer = WebContainerHelper.createNonPersistentContainer();
    }

    @AfterClass
    public static void stopServer()
    {
        if ( webContainer != null )
        {
            webContainer.shutdown();
        }
    }

    @Test
    public void shouldRedirectToBrowser() throws Exception
    {
        var response = sendGetRequest( ACCEPT, TEXT_HTML );

        assertEquals( 303, response.statusCode() );
        assertEquals( List.of( webContainer.getBaseUri() + "browser/" ), response.headers().allValues( LOCATION ) );
    }

    @Test
    public void shouldRedirectToBrowserUsingXForwardedHeaders() throws Exception
    {
        var response = sendGetRequest( ACCEPT, TEXT_HTML, "X-Forwarded-Host", "foo.bar:8734", "X-Forwarded-Proto", "https" );

        assertEquals( 303, response.statusCode() );
        assertEquals( List.of( "https://foo.bar:8734/browser/" ), response.headers().allValues( LOCATION ) );
    }

    private static HttpResponse<Void> sendGetRequest( String... headers ) throws Exception
    {
        var request = HttpRequest.newBuilder( webContainer.getBaseUri() )
                .headers( headers )
                .GET()
                .build();

        var client = HttpClient.newBuilder()
                .followRedirects( NEVER )
                .build();

        return client.send( request, discarding() );
    }
}
