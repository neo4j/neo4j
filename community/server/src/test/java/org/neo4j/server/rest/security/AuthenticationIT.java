/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.rest.security;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.RESTRequestGenerator;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.TestData;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.RawPayload;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class AuthenticationIT extends CommunityServerTestBase
{
    @Rule
    public TestData<RESTRequestGenerator> gen = TestData.producedThrough( RESTRequestGenerator.PRODUCER );

    @Test
    @Documented( "Missing authorization\n" +
                 "\n" +
                 "If an +Authorization+ header is not supplied, the server will reply with an error." )
    public void missing_authorization() throws JsonParseException, IOException
    {
        // Given
        startServerWithConfiguredUser();

        // Document
        RESTRequestGenerator.ResponseEntity response = gen.get()
                .expectedStatus( 401 )
                .expectedHeader( "WWW-Authenticate", "Basic realm=\"Neo4j\"" )
                .get( databaseURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        JsonNode firstError = data.get( "errors" ).get( 0 );
        assertThat( firstError.get( "code" ).asText(), equalTo( Status.Security.Unauthorized.code().serialize() ) );
        assertThat( firstError.get( "message" ).asText(), equalTo( "No authentication header supplied." ) );
    }

    @Test
    @Documented( "Authenticate to access the server\n" +
                 "\n" +
                 "Authenticate by sending a username and a password to Neo4j using HTTP Basic Auth.\n" +
                 "Requests should include an +Authorization+ header, with a value of +Basic <payload>+,\n" +
                 "where \"payload\" is a base64 encoded string of \"username:password\"." )
    public void successful_authentication() throws JsonParseException, IOException
    {
        // Given
        startServerWithConfiguredUser();

        // Then
        HTTP.Response response = HTTP.withBasicAuth( "neo4j", "secret" ).POST( txCommitURL( "system" ), query( "SHOW USERS" ) );

        assertThat( response.status(), equalTo( 200 ) );

        final JsonNode jsonNode = getResultRow( response );
        assertThat( jsonNode.get(0).asText(), equalTo( "neo4j" ) );
        assertThat( jsonNode.get(1).asBoolean(), equalTo( false ) );
    }

    @Test
    @Documented( "Incorrect authentication\n" +
                 "\n" +
                 "If an incorrect username or password is provided, the server replies with an error." )
    public void incorrect_authentication() throws JsonParseException, IOException
    {
        // Given
        startServerWithConfiguredUser();

        // Document
        RESTRequestGenerator.ResponseEntity response = gen.get()
                .expectedStatus( 401 )
                .withHeader( HttpHeaders.AUTHORIZATION, HTTP.basicAuthHeader( "neo4j", "incorrect" ) )
                .expectedHeader( "WWW-Authenticate", "Basic realm=\"Neo4j\"" )
                .post( databaseURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        JsonNode firstError = data.get( "errors" ).get( 0 );
        assertThat( firstError.get( "code" ).asText(), equalTo( Status.Security.Unauthorized.code().serialize() ) );
        assertThat( firstError.get( "message" ).asText(), equalTo( "Invalid username or password." ) );
    }

    @Test
    @Documented( "Required password changes\n" +
                 "\n" +
                 "In some cases, like the very first time Neo4j is accessed, the user will be required to choose\n" +
                 "a new password. The database will signal that a new password is required and deny access.\n" +
                 "\n" +
                 "See <<rest-api-security-user-status-and-password-changing>> for how to set a new password." )
    public void password_change_required() throws JsonParseException, IOException
    {
        // Given
        startServer( true );

        // It should be possible to authenticate with password change required
        gen.get().expectedStatus( 200 ).withHeader( HttpHeaders.AUTHORIZATION, HTTP.basicAuthHeader( "neo4j", "neo4j" ) );

        // When
        HTTP.Response responseBeforePasswordChange = HTTP.withBasicAuth( "neo4j", "neo4j" ).POST( txCommitURL( "system" ), query( "SHOW USERS" ) );

        // Then
        // The server should throw error when trying to do something else than changing password
        assertPermissionErrorAtSystemAccess( responseBeforePasswordChange );

        // When
        // Changing the user password
        HTTP.Response response =
                HTTP.withBasicAuth( "neo4j", "neo4j" ).POST( txCommitURL( "system" ), query( "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secret'" ) );
        // Then
        assertThat( response.status(), equalTo( 200 ) );
        assertThat( "Should have no errors", response.get( "errors" ).size(), equalTo( 0 ) );

        // When
        HTTP.Response responseAfterPasswordChange = HTTP.withBasicAuth( "neo4j", "secret" ).POST( txCommitURL( "system" ), query( "SHOW USERS" ) );

        // Then
        assertThat( responseAfterPasswordChange.status(), equalTo( 200 ) );
        assertThat( "Should have no errors", response.get( "errors" ).size(), equalTo( 0 ) );
    }

    @Test
    public void shouldSayMalformedHeaderIfMalformedAuthorization() throws Exception
    {
        // Given
        startServerWithConfiguredUser();

        // When
        HTTP.Response response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, "This makes no sense" ).GET( databaseURL() );

        // Then
        assertThat( response.status(), equalTo( 400 ) );
        assertThat( response.get( "errors" ).get( 0 ).get( "code" ).asText(), equalTo( Status.Request.InvalidFormat.code().serialize() ) );
        assertThat( response.get( "errors" ).get( 0 ).get( "message" ).asText(), equalTo( "Invalid authentication header." ) );
    }

    @Test
    public void shouldAllowDataAccess() throws Exception
    {
        // Given
        startServerWithConfiguredUser();

        // When & then
        assertAuthorizationRequired( "POST", txCommitEndpoint(), RawPayload.quotedJson(
                "{'statements':[{'statement':'MATCH (n) RETURN n'}]}" ), 200 );
        assertAuthorizationRequired( "GET", "db/data/nowhere", null, 404 );

        assertEquals(200, HTTP.GET( server.baseUri().resolve( "" ).toString() ).status() );
    }

    @Test
    public void shouldAllowAllAccessIfAuthenticationIsDisabled() throws Exception
    {
        // Given
        startServer( false );

        // When & then
        assertEquals( 200, HTTP.POST( txCommitURL(),
                RawPayload.quotedJson( "{'statements':[{'statement':'MATCH (n) RETURN n'}]}" ) ).status() );
        assertEquals( 404, HTTP.GET( server.baseUri().resolve( "db/data/nowhere" ).toString() ).status() );
    }

    @Test
    public void shouldReplyNicelyToTooManyFailedAuthAttempts() throws Exception
    {
        // Given
        startServerWithConfiguredUser();
        long timeout = System.currentTimeMillis() + 30_000;

        // When
        HTTP.Response response = null;
        while ( System.currentTimeMillis() < timeout )
        {
            // Done in a loop because we're racing with the clock to get enough failed requests into 5 seconds
            response = HTTP.withBasicAuth( "neo4j", "incorrect" ).POST(
                    server.baseUri().resolve( "authentication" ).toString(),
                    HTTP.RawPayload.quotedJson( "{'username':'neo4j', 'password':'something that is wrong'}" )
            );

            if ( response.status() == 429 )
            {
                break;
            }
        }

        // Then
        assertNotNull( response );
        assertThat( response.status(), equalTo( 429 ) );
        JsonNode firstError = response.get( "errors" ).get( 0 );
        assertThat( firstError.get( "code" ).asText(), equalTo( Status.Security.AuthenticationRateLimit.code().serialize() ) );
        assertThat( firstError.get( "message" ).asText(),
                equalTo( "Too many failed authentication requests. Please wait 5 seconds and try again." ) );
    }

    @Test
    public void shouldNotAllowDataAccessWhenPasswordChangeRequired() throws Exception
    {
        // Given
        startServer( true ); // The user should not have read access before changing the password

        // When
        final HTTP.Response response = HTTP.withBasicAuth( "neo4j", "neo4j" ).POST( server.baseUri().resolve( txCommitURL() ).toString(),
                RawPayload.quotedJson( "{'statements':[{'statement':'MATCH (n) RETURN n'}]}" ) );

        // Then
        assertPermissionErrorAtDataAccess( response  );
    }

    private void assertAuthorizationRequired( String method, String path, Object payload,
            int expectedAuthorizedStatus ) throws JsonParseException
    {
        // When no header
        HTTP.Response response = HTTP.request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat( response.status(), equalTo( 401 ) );
        assertThat( response.get( "errors" ).get( 0 ).get( "code" ).asText(), equalTo( Status.Security.Unauthorized.code().serialize() ) );
        assertThat( response.get( "errors" ).get( 0 ).get( "message" ).asText(), equalTo( "No authentication header supplied." ) );
        assertThat( response.header( HttpHeaders.WWW_AUTHENTICATE ), equalTo( "Basic realm=\"Neo4j\"" ) );

        // When malformed header
        response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, "This makes no sense" )
                .request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat( response.status(), equalTo( 400 ) );
        assertThat( response.get( "errors" ).get( 0 ).get( "code" ).asText(), equalTo( Status.Request.InvalidFormat.code().serialize() ) );
        assertThat( response.get( "errors" ).get( 0 ).get( "message" ).asText(), equalTo( "Invalid authentication header." ) );

        // When invalid credential
        response = HTTP.withBasicAuth( "neo4j", "incorrect" )
                .request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat( response.status(), equalTo( 401 ) );
        assertThat( response.get( "errors" ).get( 0 ).get( "code" ).asText(), equalTo( Status.Security.Unauthorized.code().serialize() ) );
        assertThat( response.get( "errors" ).get( 0 ).get( "message" ).asText(), equalTo( "Invalid username or password." ) );
        assertThat( response.header( HttpHeaders.WWW_AUTHENTICATE ), equalTo( "Basic realm=\"Neo4j\"" ) );

        // When authorized
        response = HTTP.withBasicAuth( "neo4j", "secret" )
                .request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat( response.status(), equalTo( expectedAuthorizedStatus ) );
    }

    protected void startServerWithConfiguredUser() throws IOException
    {
        startServer( true );
        // Set the password
        HTTP.Response post = HTTP.withBasicAuth( "neo4j", "neo4j" ).POST( txCommitURL( "system" ),
                query("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secret'" ) );
        assertEquals( 200, post.status() );
    }

    private JsonNode getResultRow( HTTP.Response response ) throws JsonParseException
    {
        return response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "row" );
    }
}
