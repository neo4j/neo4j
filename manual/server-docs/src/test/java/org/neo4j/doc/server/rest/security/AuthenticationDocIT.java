/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.doc.server.rest.security;

import com.sun.jersey.core.util.Base64;
import org.codehaus.jackson.JsonNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.doc.server.rest.RESTDocsGenerator;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.TestData;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.RawPayload;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class AuthenticationDocIT extends ExclusiveServerTestBase
{
    public @Rule TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );
    private CommunityNeoServer server;

    @Before
    public void setUp()
    {
        gen.get().setSection( "dev/rest-api" );
    }

    @Test
    @Documented( "Missing authorization\n" +
                 "\n" +
                 "If an +Authorization+ header is not supplied, the server will reply with an error." )
    public void missing_authorization() throws JsonParseException, IOException
    {
        // Given
        startServerWithConfiguredUser();

        // Document
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 401 )
                .expectedHeader( "WWW-Authenticate", "Basic realm=\"Neo4j\"" )
                .get( dataURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        JsonNode firstError = data.get( "errors" ).get( 0 );
        assertThat( firstError.get( "code" ).asText(), equalTo( "Neo.ClientError.Security.AuthorizationFailed" ) );
        assertThat( firstError.get( "message" ).asText(), equalTo( "No authorization header supplied." ) );
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
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .withHeader( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "secret" ) )
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
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 401 )
                .withHeader( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "incorrect" ) )
                .expectedHeader( "WWW-Authenticate", "Basic realm=\"Neo4j\"" )
                .post( dataURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        JsonNode firstError = data.get( "errors" ).get( 0 );
        assertThat( firstError.get( "code" ).asText(), equalTo( "Neo.ClientError.Security.AuthorizationFailed" ) );
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
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 403 )
                .withHeader( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "neo4j" ) )
                .get( dataURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        JsonNode firstError = data.get( "errors" ).get( 0 );
        assertThat( firstError.get( "code" ).asText(), equalTo( "Neo.ClientError.Security.AuthorizationFailed" ) );
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
            .noGraph()
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
        assertThat( response.get( "errors" ).get( 0 ).get( "message" ).asText(), equalTo( "Invalid Authorization header." ) );
    }

    @Test
    public void shouldNotAllowDataAccess() throws Exception
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
            response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "incorrect" ) ).POST(
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
        assertThat( firstError.get( "message" ).asText(), equalTo( "Too many failed authentication requests. Please wait 5 seconds and try again." ) );
    }

    private void assertAuthorizationRequired( String method, String path, int expectedAuthorizedStatus ) throws JsonParseException
    {
        assertAuthorizationRequired( method, path, null, expectedAuthorizedStatus );
    }

    private void assertAuthorizationRequired( String method, String path, Object payload, int expectedAuthorizedStatus ) throws JsonParseException
    {
        // When no header
        HTTP.Response response = HTTP.request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat(response.status(), equalTo(401));
        assertThat(response.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Security.AuthorizationFailed"));
        assertThat(response.get("errors").get(0).get("message").asText(), equalTo("No authorization header supplied."));
        assertThat(response.header( HttpHeaders.WWW_AUTHENTICATE ), equalTo("Basic realm=\"Neo4j\""));

        // When malformed header
        response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, "This makes no sense" ).request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat(response.status(), equalTo(400));
        assertThat(response.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Request.InvalidFormat"));
        assertThat(response.get("errors").get(0).get( "message" ).asText(), equalTo("Invalid Authorization header."));

        // When invalid credential
        response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "incorrect" ) ).request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat(response.status(), equalTo(401));
        assertThat(response.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Security.AuthorizationFailed"));
        assertThat(response.get("errors").get(0).get("message").asText(), equalTo("Invalid username or password."));
        assertThat(response.header(HttpHeaders.WWW_AUTHENTICATE ), equalTo("Basic realm=\"Neo4j\""));

        // When authorized
        response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "secret" ) ).request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat(response.status(), equalTo(expectedAuthorizedStatus));
    }

    @After
    public void cleanup()
    {
        if(server != null) {server.stop();}
    }

    public void startServerWithConfiguredUser() throws IOException
    {
        startServer( true );
        // Set the password
        HTTP.Response post = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "neo4j" ) ).POST(
                server.baseUri().resolve( "/user/neo4j/password" ).toString(),
                RawPayload.quotedJson( "{'password':'secret'}" )
        );
        assertEquals( 200, post.status() );
    }

    public void startServer( boolean authEnabled ) throws IOException
    {
        File authStore = ServerTestUtils.getRelativeFile( ServerSettings.auth_store );
        FileUtils.deleteFile( authStore);
        server = CommunityServerBuilder.server()
                .withProperty( ServerSettings.auth_enabled.name(), Boolean.toString( authEnabled ) )
                .withProperty( ServerSettings.auth_store.name(), authStore.getAbsolutePath() )
                .build();
        server.start();
    }

    private String challengeResponse( String username, String password )
    {
        return "Basic " + base64( username + ":" + password );
    }

    private String dataURL()
    {
        return server.baseUri().resolve( "db/data/" ).toString();
    }

    private String userURL( String username )
    {
        return server.baseUri().resolve( "user/" + username ).toString();
    }

    private String passwordURL( String username )
    {
        return server.baseUri().resolve( "user/" + username + "/password" ).toString();
    }

    private String base64(String value)
    {
        return UTF8.decode( Base64.encode( value ) );
    }
}
