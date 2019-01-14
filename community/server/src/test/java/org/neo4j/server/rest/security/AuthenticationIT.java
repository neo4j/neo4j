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
package org.neo4j.server.rest.security;

import org.codehaus.jackson.JsonNode;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.RESTRequestGenerator;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.TestData;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.RawPayload;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
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
                .get( dataURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        JsonNode firstError = data.get( "errors" ).get( 0 );
        assertThat( firstError.get( "code" ).asText(), equalTo( "Neo.ClientError.Security.Unauthorized" ) );
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

        // Document
        RESTRequestGenerator.ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .withHeader( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "secret" ) )
                .get( userURL( "neo4j" ) );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        assertThat( data.get( "username" ).asText(), equalTo( "neo4j" ) );
        assertThat( data.get( "password_change_required" ).asBoolean(), equalTo( false ) );
        assertThat( data.get( "password_change" ).asText(), equalTo( passwordURL( "neo4j" ) ) );
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
                .withHeader( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "incorrect" ) )
                .expectedHeader( "WWW-Authenticate", "Basic realm=\"Neo4j\"" )
                .post( dataURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        JsonNode firstError = data.get( "errors" ).get( 0 );
        assertThat( firstError.get( "code" ).asText(), equalTo( "Neo.ClientError.Security.Unauthorized" ) );
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

        // Document
        RESTRequestGenerator.ResponseEntity response = gen.get()
                .expectedStatus( 403 )
                .withHeader( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "neo4j" ) )
                .get( dataURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        JsonNode firstError = data.get( "errors" ).get( 0 );
        assertThat( firstError.get( "code" ).asText(), equalTo( "Neo.ClientError.Security.Forbidden" ) );
        assertThat( firstError.get( "message" ).asText(), equalTo( "User is required to change their password." ) );
        assertThat( data.get( "password_change" ).asText(), equalTo( passwordURL( "neo4j" ) ) );
    }

    @Test
    @Documented( "When auth is disabled\n" +
                 "\n" +
                 "When auth has been disabled in the configuration, requests can be sent without an +Authorization+ header." )
    public void auth_disabled() throws IOException
    {
        // Given
        startServer( false );

        // Document
        gen.get()
            .expectedStatus( 200 )
            .get( dataURL() );
    }

    @Test
    public void shouldSayMalformedHeaderIfMalformedAuthorization() throws Exception
    {
        // Given
        startServerWithConfiguredUser();

        // When
        HTTP.Response response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, "This makes no sense" ).GET( dataURL() );

        // Then
        assertThat( response.status(), equalTo( 400 ) );
        assertThat( response.get( "errors" ).get( 0 ).get( "code" ).asText(), equalTo( "Neo.ClientError.Request.InvalidFormat" ) );
        assertThat( response.get( "errors" ).get( 0 ).get( "message" ).asText(), equalTo( "Invalid authentication header." ) );
    }

    @Test
    public void shouldAllowDataAccess() throws Exception
    {
        // Given
        startServerWithConfiguredUser();

        // When & then
        assertAuthorizationRequired( "POST", "db/data/node", RawPayload.quotedJson( "{'name':'jake'}" ), 201 );
        assertAuthorizationRequired( "GET",  "db/data/node/1234", 404 );
        assertAuthorizationRequired( "POST", "db/data/transaction/commit", RawPayload.quotedJson(
                "{'statements':[{'statement':'MATCH (n) RETURN n'}]}" ), 200 );

        assertEquals(200, HTTP.GET( server.baseUri().resolve( "" ).toString() ).status() );
    }

    @Test
    public void shouldAllowAllAccessIfAuthenticationIsDisabled() throws Exception
    {
        // Given
        startServer( false );

        // When & then
        assertEquals( 201, HTTP.POST( server.baseUri().resolve( "db/data/node" ).toString(),
                RawPayload.quotedJson( "{'name':'jake'}" ) ).status() );
        assertEquals( 404, HTTP.GET( server.baseUri().resolve( "db/data/node/1234" ).toString() ).status() );
        assertEquals( 200, HTTP.POST( server.baseUri().resolve( "db/data/transaction/commit" ).toString(),
                RawPayload.quotedJson( "{'statements':[{'statement':'MATCH (n) RETURN n'}]}" ) ).status() );
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
            response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "incorrect" ) ).POST(
                    server.baseUri().resolve( "authentication" ).toString(),
                    HTTP.RawPayload.quotedJson( "{'username':'neo4j', 'password':'something that is wrong'}" )
            );

            if ( response.status() == 429 )
            {
                break;
            }
        }

        // Then
        assertThat( response.status(), equalTo( 429 ) );
        JsonNode firstError = response.get( "errors" ).get( 0 );
        assertThat( firstError.get( "code" ).asText(), equalTo( "Neo.ClientError.Security.AuthenticationRateLimit" ) );
        assertThat( firstError.get( "message" ).asText(),
                equalTo( "Too many failed authentication requests. Please wait 5 seconds and try again." ) );
    }

    @Test
    public void shouldNotAllowDataAccessForUnauthorizedUser() throws Exception
    {
        // Given
        startServer( true ); // The user should not have read access before changing the password

        // When
        HTTP.Response response =
                HTTP.withHeaders( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "neo4j" ) ).POST(
                        server.baseUri().resolve( "authentication" ).toString(),
                        HTTP.RawPayload.quotedJson( "{'username':'neo4j', 'password':'neo4j'}" )
                );

        // When & then
        assertEquals( 403, HTTP.withHeaders( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "neo4j" ) )
                .POST( server.baseUri().resolve( "db/data/node" ).toString(),
                        RawPayload.quotedJson( "{'name':'jake'}" ) ).status() );
        assertEquals( 403, HTTP.withHeaders( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "neo4j" ) )
                .GET( server.baseUri().resolve( "db/data/node/1234" ).toString() ).status() );
        assertEquals( 403, HTTP.withHeaders( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "neo4j" ) )
                .POST( server.baseUri().resolve( "db/data/transaction/commit" ).toString(),
                        RawPayload.quotedJson( "{'statements':[{'statement':'MATCH (n) RETURN n'}]}" ) ).status() );
    }

    private void assertAuthorizationRequired( String method, String path, int expectedAuthorizedStatus ) throws JsonParseException
    {
        assertAuthorizationRequired( method, path, null, expectedAuthorizedStatus );
    }

    private void assertAuthorizationRequired( String method, String path, Object payload,
            int expectedAuthorizedStatus ) throws JsonParseException
    {
        // When no header
        HTTP.Response response = HTTP.request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat(response.status(), equalTo(401));
        assertThat(response.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Security.Unauthorized"));
        assertThat(response.get("errors").get(0).get("message").asText(), equalTo("No authentication header supplied."));
        assertThat(response.header( HttpHeaders.WWW_AUTHENTICATE ), equalTo("Basic realm=\"Neo4j\""));

        // When malformed header
        response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, "This makes no sense" )
                .request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat(response.status(), equalTo(400));
        assertThat(response.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Request.InvalidFormat"));
        assertThat(response.get("errors").get(0).get( "message" ).asText(), equalTo("Invalid authentication header."));

        // When invalid credential
        response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "incorrect" ) )
                .request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat(response.status(), equalTo(401));
        assertThat(response.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Security.Unauthorized"));
        assertThat(response.get("errors").get(0).get("message").asText(), equalTo("Invalid username or password."));
        assertThat(response.header(HttpHeaders.WWW_AUTHENTICATE ), equalTo("Basic realm=\"Neo4j\""));

        // When authorized
        response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "secret" ) )
                .request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat(response.status(), equalTo(expectedAuthorizedStatus));
    }

    public void startServerWithConfiguredUser() throws IOException
    {
        startServer( true );
        // Set the password
        HTTP.Response post = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, basicAuthHeader( "neo4j", "neo4j" ) ).POST(
                server.baseUri().resolve( "/user/neo4j/password" ).toString(),
                RawPayload.quotedJson( "{'password':'secret'}" )
        );
        assertEquals( 200, post.status() );
    }
}
