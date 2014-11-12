/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import javax.ws.rs.core.HttpHeaders;

import com.sun.jersey.core.util.Base64;
import org.codehaus.jackson.JsonNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.rest.RESTDocsGenerator;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.TestData;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;
import static org.neo4j.test.server.HTTP.RawPayload;

public class AuthenticationDocIT extends ExclusiveServerTestBase
{
    public @Rule TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );
    private CommunityNeoServer server;

    @Before
    public void setUp()
    {
        gen.get().setSection( "dev/rest-api" );
    }

    /**
     * Required password changes
     *
     * In some cases, like the very first time you access Neo4j with authorization enabled, you are required to choose
     * a new password. The database will signal that a new password is required when you try to authenticate.
     *
     * See <<rest-api-changing-your-password>> for how to set a new password.
     */
    @Test
    @Documented
    public void password_change_required() throws PropertyValueException, IOException
    {
        // Given
        startServer( true );

        // Document
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson( "{'username':'neo4j', 'password':'neo4j'}" ) )
                .post( authURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        assertThat(data.get("username").asText(), equalTo("neo4j"));
        assertThat(data.has("authorization_token" ), is( false ));
        assertThat(data.has("authorization_token_change" ), is( false ));
        assertThat(data.get("password_change_required").asBoolean(), equalTo( true ));
        assertThat(data.get("password_change").asText(), equalTo( server.baseUri().resolve("user/neo4j/password").toString() ));
    }

    /**
     * Authenticate to obtain authorization token
     *
     * You authenticate by sending a username and a password to Neo4j. The database will reply with an authorization
     * token. The reply from this endpoint will also indicate if your password should be changed which will,
     * for instance, be the case in a newly installed instance.
     */
    @Test
    @Documented
    public void successful_authentication() throws PropertyValueException, IOException
    {
        // Given
        startServerWithConfiguredUser();

        // Document
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson( "{'username':'neo4j', 'password':'secret'}" ) )
                .post( authURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        assertThat(data.get("username").asText(), equalTo("neo4j"));
        assertThat(data.get("password_change_required").asBoolean(), equalTo( false ));
        assertThat(data.get("authorization_token").asText().length(), greaterThan(0));
        assertThat(data.get("authorization_token_change").asText(), equalTo( server.baseUri().resolve("user/neo4j/authorization_token").toString() ));
    }

    /**
     * Using the Authorization Token
     *
     * Given that you have acquired an authorization token, you may use it to get access to the rest of the API.
     * To include the token in requests to the server, it should be encoded as the 'password' part of the HTTP Basic Auth scheme.
     * This means you should include a +Authorization+ header, with a value of +Basic realm="Neo4j" <token payload>+
     * where "token payload" is a base64 encoded string of the token prepended by a colon.
     *
     * In pseudo-code:
     *
     * [source,javascript]
     * ----
     * authorization = 'Basic realm="Neo4j" ' + base64( ':' + token );
     * ----
     */
    @Test
    @Documented
    public void using_the_token() throws PropertyValueException, IOException
    {
        // Given
        startServerWithConfiguredUser();
        String token = HTTP.POST( authURL(), RawPayload.quotedJson( "{'username':'neo4j','password':'secret'}" ) )
                .get( "authorization_token" ).asText();

        // Document
        gen.get()
            .noGraph()
            .expectedStatus( 200 )
            .withHeader( HttpHeaders.AUTHORIZATION, challengeResponse( token ) )
            .get( server.baseUri().resolve( "" ).toString() );
    }

    /**
     * Incorrect username or password
     *
     * If you provide incorrect authentication credentials, the server replies with a an error.
     */
    @Test
    @Documented
    public void incorrect_authentication() throws PropertyValueException, IOException
    {
        // Given
        startServerWithConfiguredUser();

        // Document
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 422 )
                .payload( quotedJson( "{'username':'bob', 'password':'incorrect'}" ) )
                .post( authURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        assertThat(data.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Security.AuthenticationFailed"));
        assertThat(data.get("errors").get(0).get( "message" ).asText(), equalTo("Invalid username and/or password."));
    }

    /**
     * Get current authorization status
     *
     * You can use this endpoint to determine if security is enabled, and to check if your authorization token is valid.
     *
     * Given that you have a valid authorization token, you can retrieve metadata about the current user from the authentication endpoint.
     * If neo4j security is disabled, this endpoint will also return 200 OK, see <<rest-api-getting-authorization-status-when-auth-is-disabled>>.
     * If security is enabled and your token is invalid, you will get an error reply, see <<rest-api-attempting-to-get-authorization-status-while-unauthorized>>.
     * This way, you can use this endpoint to determine if you need to acquire an authorization token.
     */
    @Test
    @Documented
    public void authorization_metadata() throws PropertyValueException, IOException
    {
        // Given
        startServerWithConfiguredUser();
        String authToken = HTTP.POST( authURL(), RawPayload.quotedJson( "{'username':'neo4j','password':'secret'}" ) )
                .get( "authorization_token" ).asText();

        // Document
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .withHeader( HttpHeaders.AUTHORIZATION, challengeResponse( authToken ) )
                .get( authURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        assertThat(data.get( "username" ).asText(), equalTo("neo4j"));
    }

    /**
     * Attempting to get authorization status while unauthorized
     *
     * Given that you have an invalid authorization token, or no token at all, asking for authorization status leads
     * to an unauthorized HTTP reply.
     */
    @Test
    @Documented
    public void disallowed_authorization_metadata() throws PropertyValueException, IOException
    {
        // Given
        startServerWithConfiguredUser();

        // Document
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 401 )
                .expectedHeader( "WWW-Authenticate", "None" )
                .withHeader( HttpHeaders.AUTHORIZATION, "Basic realm=\"Neo4j\" " + base64( ":helloworld" ) )
                .get( authURL() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        assertThat(data.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Security.AuthorizationFailed"));
        assertThat( data.get( "errors" ).get( 0 ).get( "message" ).asText(), equalTo( "Invalid authorization token supplied." ) );
    }

    /**
     * Get authorization status when auth is disabled
     *
     * Given that auth is disabled in the configuration, you can perform a GET to the authentication endpoint, and will
     * get back an OK response. You will not receive a username or authorization token.
     */
    @Test
    @Documented
    public void auth_disabled_get_metadata() throws PropertyValueException, IOException
    {
        // Given
        startServer(false);

        // Document
        gen.get()
            .noGraph()
            .expectedStatus( 200 )
            .get( authURL() );
    }

    @Test
    public void shouldSayTokenMissingIfNoTokenProvided() throws Exception
    {
        // Given
        startServerWithConfiguredUser();

        // When
        HTTP.Response response = HTTP.GET( authURL() );

        // Then
        assertThat(response.status(), equalTo(401));
        assertThat(response.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Security.AuthorizationFailed"));
        assertThat(response.get("errors").get(0).get("message").asText(), equalTo("No authorization token supplied."));
        assertThat(response.header("WWW-Authenticate"), equalTo("None"));
    }

    @Test
    public void shouldSayMalformedTokenIfMalformedToken() throws Exception
    {
        // Given
        startServerWithConfiguredUser();

        // When
        HTTP.Response response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, "This makes no sense" ).GET( authURL() );

        // Then
        assertThat(response.status(), equalTo(400));
        assertThat(response.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Request.InvalidFormat"));
        assertThat(response.get("errors").get(0).get("message").asText(), equalTo("Invalid Authorization header."));
    }

    @Test
    public void shouldHandleMissingParameters() throws Exception
    {
        // Given
        startServerWithConfiguredUser();

        // When & Then
        assertEquals( 422, HTTP.POST( authURL() ).status() );
        assertEquals( 422, HTTP.POST( authURL(), RawPayload.quotedJson("{'password':'whatever'}") ).status() );
        assertEquals( 422, HTTP.POST( authURL(), RawPayload.quotedJson("{'password':1234, 'username':{}}") ).status() );
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
        assertEquals(200, HTTP.GET( server.baseUri().resolve( "webadmin" ).toString() ).status());
        assertEquals(200, HTTP.GET( server.baseUri().resolve( "browser" ).toString() ).status());
        assertEquals(200, HTTP.GET( server.baseUri().resolve( "" ).toString() ).status() );
    }

    @Test
    public void rootEndpointShouldOnlyShowAuthenticationDiscoverabilityUrl() throws Exception
    {
        // Given
        startServerWithConfiguredUser();

        // When
        HTTP.Response res = HTTP.GET( server.baseUri().resolve( "" ).toString() );

        // Then
        assertThat( res.rawContent(), equalTo(
            "{\n" +
            "  \"authentication\" : \""+server.baseUri().resolve( "authentication" )+"\"\n" +
            "}" ));
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
        while(System.currentTimeMillis() < timeout)
        {
            // Done in a loop because we're racing with the clock to get enough failed requests into 5 seconds
            response = HTTP.POST( server.baseUri().resolve( "authentication" ).toString(),
                    HTTP.RawPayload.quotedJson( "{'username':'neo4j', 'password':'something that is wrong'}" ) );

            if(response.status() == 429)
            {
                break;
            }
        }

        // Then
        assertThat(response.status(), equalTo(429));
        assertThat(response.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Security.AuthenticationRateLimit"));
        assertThat(response.get("errors").get(0).get("message").asText(), equalTo("Too many failed authentication requests. Please try again in 5 seconds."));
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
        assertThat(response.get("errors").get(0).get("message").asText(), equalTo("No authorization token supplied."));
        assertThat(response.get("authentication").asText(), equalTo("http://localhost:7474/authentication"));
        assertThat(response.header( HttpHeaders.WWW_AUTHENTICATE ), equalTo("None"));

        // When malformed header
        response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, "This makes no sense" ).request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat(response.status(), equalTo(400));
        assertThat(response.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Request.InvalidFormat"));
        assertThat(response.get("errors").get(0).get( "message" ).asText(), equalTo("Invalid Authorization header."));

        // When invalid token
        response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION,  "Basic realm=\"Neo4j\" " + base64( ":helloworld" ) ).request( method, server.baseUri().resolve( path ).toString(), payload );
        assertThat(response.status(), equalTo(401));
        assertThat(response.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Security.AuthorizationFailed"));
        assertThat(response.get("errors").get(0).get("message").asText(), equalTo("Invalid authorization token supplied."));
        assertThat(response.get("authentication").asText(), equalTo("http://localhost:7474/authentication"));
        assertThat(response.header(HttpHeaders.WWW_AUTHENTICATE ), equalTo("None"));

        // When authorized
        String token = HTTP.POST( authURL(), RawPayload.quotedJson( "{'username':'neo4j','password':'secret'}" ) )
                .get( "authorization_token" ).asText();
        response = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( token ) ).request( method, server.baseUri().resolve( path ).toString(), payload );
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
        HTTP.Response put = HTTP.POST( server.baseUri().resolve( "/user/neo4j/password" ).toString(),
                RawPayload.quotedJson( "{'password':'neo4j', 'new_password':'secret'}" ) );
        assertEquals( 200, put.status() );
    }

    public void startServer(boolean authEnabled) throws IOException
    {
        new File( "neo4j-home/data/dbms/authorization" ).delete(); // TODO: Implement a common component for managing Neo4j file structure and use that here
        server = CommunityServerBuilder.server()
                .withProperty( Configurator.AUTHORIZATION_ENABLED_PROPERTY_KEY, Boolean.toString( authEnabled ) )
                .build();
        server.start();
    }

    private String challengeResponse( String token )
    {
        return "Basic realm=\"Neo4j\" " + base64( ":" + token );
    }

    private String authURL()
    {
        return server.baseUri().resolve("authentication").toString();
    }

    private String base64(String value)
    {
        return new String( Base64.encode( value ), Charset
                .forName( "UTF-8" ));
    }

    private String quotedJson( String singleQuoted )
    {
        return singleQuoted.replaceAll( "'", "\"" );
    }
}