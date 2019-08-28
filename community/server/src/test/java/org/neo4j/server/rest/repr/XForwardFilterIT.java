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
package org.neo4j.server.rest.repr;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.net.http.HttpRequest;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.GraphDbHelper;

import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpRequest.BodyPublishers.ofString;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class XForwardFilterIT extends AbstractRestFunctionalTestBase
{
    private static final String X_FORWARDED_HOST = "X-Forwarded-Host";
    private static final String X_FORWARDED_PROTO = "X-Forwarded-Proto";

    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer()
    {
        helper = new FunctionalTestHelper( server() ).getGraphDbHelper();
    }

    @Before
    public void setupTheDatabase()
    {
        helper.createRelationship( "RELATES_TO", helper.createNode(), helper.createNode() );
    }

    @Test
    public void shouldUseXForwardedHostHeaderWhenPresent() throws Exception
    {
        var entity = sendGetRequest( X_FORWARDED_HOST, "jimwebber.org" );

        assertTrue( entity.contains( "http://jimwebber.org" ) );
        assertFalse( entity.contains( "http://localhost" ) );
    }

    @Test
    public void shouldUseXForwardedProtoHeaderWhenPresent() throws Exception
    {
        var entity = sendGetRequest( X_FORWARDED_PROTO, "https" );

        assertTrue( entity.contains( "https://localhost" ) );
        assertFalse( entity.contains( "http://localhost" ) );
    }

    @Test
    public void shouldPickFirstXForwardedHostHeaderValueFromCommaOrCommaAndSpaceSeparatedList() throws Exception
    {
        var entity = sendGetRequest( X_FORWARDED_HOST, "jimwebber.org, kathwebber.com,neo4j.org" );

        assertTrue( entity.contains( "http://jimwebber.org" ) );
        assertFalse( entity.contains( "http://localhost" ) );
    }

    @Test
    public void shouldUseBaseUriOnBadXForwardedHostHeader() throws Exception
    {
        var entity = sendGetRequest( X_FORWARDED_HOST, ":bad_URI" );

        assertTrue( entity.contains( serverUriString() ) );
    }

    @Test
    public void shouldUseBaseUriIfFirstAddressInXForwardedHostHeaderIsBad() throws Exception
    {
        var entity = sendGetRequest( X_FORWARDED_HOST, ":bad_URI,good-host" );

        assertTrue( entity.contains( serverUriString() ) );
    }

    @Test
    public void shouldUseBaseUriOnBadXForwardedProtoHeader() throws Exception
    {
        var entity = sendGetRequest( X_FORWARDED_PROTO, "%%%DEFINITELY-NOT-A-PROTO!" );

        assertTrue( entity.contains( serverUriString() ) );
    }

    @Test
    public void shouldUseXForwardedHostAndXForwardedProtoHeadersWhenPresent() throws Exception
    {
        var entity = sendGetRequest( X_FORWARDED_HOST, "jimwebber.org",
                X_FORWARDED_PROTO, "https" );

        assertTrue( entity.contains( "https://jimwebber.org" ) );
        assertFalse( entity.contains( serverUriString() ) );
    }

    @Test
    public void shouldUseXForwardedHostAndXForwardedProtoHeadersInCypherResponseRepresentations() throws Exception
    {
        String jsonString = "{\"statements\" : [{ \"statement\": \"MATCH (n) RETURN n\", " +
                "\"resultDataContents\":[\"REST\"] }] }";

        var entity = sendPostRequest( txUri(), jsonString,
                X_FORWARDED_HOST, "jimwebber.org:2354",
                X_FORWARDED_PROTO, "https" );

        assertTrue( entity.contains( "https://jimwebber.org:2354" ) );
        assertFalse( entity.contains( serverUriString() ) );
    }

    private static String sendGetRequest( String... headers ) throws Exception
    {
        var request = HttpRequest.newBuilder( serverUri() )
                .header( ACCEPT, APPLICATION_JSON )
                .headers( headers )
                .GET()
                .build();

        return newHttpClient().send( request, ofString() ).body();
    }

    private static String sendPostRequest( String uri, String payload, String... headers ) throws Exception
    {
        var request = HttpRequest.newBuilder( URI.create( uri ) )
                .header( ACCEPT, APPLICATION_JSON )
                .header( CONTENT_TYPE, APPLICATION_JSON )
                .headers( headers )
                .POST( ofString( payload ) )
                .build();

        return newHttpClient().send( request, ofString() ).body();
    }

    private static String serverUriString()
    {
        return serverUri().toString();
    }

    private static URI serverUri()
    {
        return server().baseUri();
    }
}
