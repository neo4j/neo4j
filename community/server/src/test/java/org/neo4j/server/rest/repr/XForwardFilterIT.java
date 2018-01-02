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
package org.neo4j.server.rest.repr;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import javax.ws.rs.core.MediaType;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.GraphDbHelper;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class XForwardFilterIT extends AbstractRestFunctionalTestBase
{

    public static final String X_FORWARDED_HOST = "X-Forwarded-Host";
    public static final String X_FORWARDED_PROTO = "X-Forwarded-Proto";
    private Client client = Client.create();

    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        FunctionalTestHelper functionalTestHelper = new FunctionalTestHelper( server() );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @Before
    public void setupTheDatabase()
    {
        helper.createRelationship( "RELATES_TO", helper.createNode(), helper.createNode() );
    }

    @Test
    public void shouldUseXForwardedHostHeaderWhenPresent() throws Exception
    {
        // when
        ClientResponse response = client.resource( "http://localhost:7474/db/manage" )
                .accept( APPLICATION_JSON )
                .header( X_FORWARDED_HOST, "jimwebber.org" )
                .get( ClientResponse.class );

        // then
        String entity = response.getEntity( String.class );
        assertTrue( entity.contains( "http://jimwebber.org" ) );
        assertFalse( entity.contains( "http://localhost" ) );
    }

    @Test
    public void shouldUseXForwardedProtoHeaderWhenPresent() throws Exception
    {
        // when
        ClientResponse response = client.resource( "http://localhost:7474/db/manage" )
                .accept( APPLICATION_JSON )
                .header( X_FORWARDED_PROTO, "https" )
                .get( ClientResponse.class );

        // then
        String entity = response.getEntity( String.class );
        assertTrue( entity.contains( "https://localhost" ) );
        assertFalse( entity.contains( "http://localhost" ) );
    }

    @Test
    public void shouldPickFirstXForwardedHostHeaderValueFromCommaOrCommaAndSpaceSeparatedList() throws Exception
    {
        // when
        ClientResponse response = client.resource( "http://localhost:7474/db/manage" )
                .accept( APPLICATION_JSON )
                .header( X_FORWARDED_HOST, "jimwebber.org, kathwebber.com,neo4j.org" )
                .get( ClientResponse.class );

        // then
        String entity = response.getEntity( String.class );
        assertTrue( entity.contains( "http://jimwebber.org" ) );
        assertFalse( entity.contains( "http://localhost" ) );
    }

    @Test
    public void shouldUseBaseUriOnBadXForwardedHostHeader() throws Exception
    {
        // when
        ClientResponse response = client.resource( "http://localhost:7474/db/manage" )
                .accept( APPLICATION_JSON )
                .header( X_FORWARDED_HOST, ":bad_URI" )
                .get( ClientResponse.class );

        // then
        String entity = response.getEntity( String.class );
        assertTrue( entity.contains( "http://localhost:7474" ) );
    }

    @Test
    public void shouldUseBaseUriIfFirstAddressInXForwardedHostHeaderIsBad() throws Exception
    {
        // when
        ClientResponse response = client.resource( "http://localhost:7474/db/manage" )
                .accept( APPLICATION_JSON )
                .header( X_FORWARDED_HOST, ":bad_URI,good-host" )
                .get( ClientResponse.class );

        // then
        String entity = response.getEntity( String.class );
        assertTrue( entity.contains( "http://localhost:7474" ) );
    }

    @Test
    public void shouldUseBaseUriOnBadXForwardedProtoHeader() throws Exception
    {
        // when
        ClientResponse response = client.resource( "http://localhost:7474/db/manage" )
                .accept( APPLICATION_JSON )
                .header( X_FORWARDED_PROTO, "%%%DEFINITELY-NOT-A-PROTO!" )
                .get( ClientResponse.class );

        // then
        String entity = response.getEntity( String.class );
        assertTrue( entity.contains( "http://localhost:7474" ) );
    }

    @Test
    public void shouldUseXForwardedHostAndXForwardedProtoHeadersWhenPresent() throws Exception
    {
        // when
        ClientResponse response = client.resource( "http://localhost:7474/db/manage" )
                .accept( APPLICATION_JSON )
                .header( X_FORWARDED_HOST, "jimwebber.org" )
                .header( X_FORWARDED_PROTO, "https" )
                .get( ClientResponse.class );

        // then
        String entity = response.getEntity( String.class );
        assertTrue( entity.contains( "https://jimwebber.org" ) );
        assertFalse( entity.contains( "http://localhost:7474" ) );
    }

    @Test
    public void shouldUseXForwardedHostAndXForwardedProtoHeadersInCypherResponseRepresentations()
    {
        // when
        String jsonString = "{\"statements\" : [{ \"statement\": \"MATCH (n) RETURN n\", " +
                "\"resultDataContents\":[\"REST\"] }] }";

        ClientResponse response = client.resource( "http://localhost:7474/db/data/transaction" )
                .accept( APPLICATION_JSON )
                .header( X_FORWARDED_HOST, "jimwebber.org:2354" )
                .header( X_FORWARDED_PROTO, "https" )
                .entity( jsonString, MediaType.APPLICATION_JSON_TYPE )
                .post( ClientResponse.class );

        // then
        String entity = response.getEntity( String.class );
        assertTrue( entity.contains( "https://jimwebber.org:2354" ) );
        assertFalse( entity.contains( "http://localhost:7474" ) );
    }
}
