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
package org.neo4j.server.rest.security;

import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;

import org.neo4j.test.server.HTTP;

import static com.sun.jersey.api.client.ClientResponse.Status.FORBIDDEN;
import static com.sun.jersey.api.client.ClientResponse.Status.OK;
import static com.sun.jersey.api.client.ClientResponse.Status.UNAUTHORIZED;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class AuthorizationCorsIT extends CommunityServerTestBase
{
    @Test
    public void shouldAddCorsHeaderWhenAuthDisabled() throws Exception
    {
        startServer( false );

        HTTP.Response response = runQuery( "authDisabled", "authDisabled" );

        assertEquals( OK.getStatusCode(), response.status() );
        assertCorsHeaderPresent( response );
        assertThat( response.content().toString(), containsString( "42" ) );
    }

    @Test
    public void shouldAddCorsHeaderWhenAuthEnabledAndPasswordChangeRequired() throws Exception
    {
        startServer( true );

        HTTP.Response response = runQuery( "neo4j", "neo4j" );

        assertEquals( FORBIDDEN.getStatusCode(), response.status() );
        assertCorsHeaderPresent( response );
        assertThat( response.content().toString(), containsString( "password_change" ) );
    }

    @Test
    public void shouldAddCorsHeaderWhenAuthEnabledAndPasswordChangeNotRequired() throws Exception
    {
        startServer( true );
        HTTP.Response passwordChangeResponse = changePassword( "neo4j", "neo4j", "newPassword" );
        assertEquals( OK.getStatusCode(), passwordChangeResponse.status() );
        assertCorsHeaderPresent( passwordChangeResponse );

        HTTP.Response queryResponse = runQuery( "neo4j", "newPassword" );

        assertEquals( OK.getStatusCode(), queryResponse.status() );
        assertCorsHeaderPresent( queryResponse );
        assertThat( queryResponse.content().toString(), containsString( "42" ) );
    }

    @Test
    public void shouldAddCorsHeaderWhenAuthEnabledAndIncorrectPassword() throws Exception
    {
        startServer( true );

        HTTP.Response response = runQuery( "neo4j", "wrongPassword" );

        assertEquals( UNAUTHORIZED.getStatusCode(), response.status() );
        assertCorsHeaderPresent( response );
        assertThat( response.content().toString(), containsString( "Neo.ClientError.Security.Unauthorized" ) );
    }

    private HTTP.Response changePassword( String username, String oldPassword, String newPassword )
    {
        HTTP.RawPayload passwordChange = quotedJson( "{'password': '" + newPassword + "'}" );
        return requestWithHeaders( username, oldPassword ).POST( passwordURL( username ), passwordChange );
    }

    private HTTP.Response runQuery( String username, String password )
    {
        HTTP.RawPayload statements = quotedJson( "{'statements': [{'statement': 'RETURN 42'}]}" );
        return requestWithHeaders( username, password ).POST( txCommitURL(), statements );
    }

    private HTTP.Builder requestWithHeaders( String username, String password )
    {
        return HTTP.withHeaders(
                HttpHeaders.ACCEPT, "application/json; charset=UTF-8",
                HttpHeaders.CONTENT_TYPE, "application/json",
                HttpHeaders.AUTHORIZATION, basicAuthHeader( username, password )
        );
    }

    private static void assertCorsHeaderPresent( HTTP.Response response )
    {
        assertEquals( "*", response.header( "Access-Control-Allow-Origin" ) );
    }
}
