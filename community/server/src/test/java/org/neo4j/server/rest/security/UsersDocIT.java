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
import java.net.URI;
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
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.rest.RESTDocsGenerator;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.TestData;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

public class UsersDocIT extends ExclusiveServerTestBase
{
    public @Rule TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );
    private CommunityNeoServer server;

    @Before
    public void setUp()
    {
        gen.get().setSection( "dev/rest-api" );
    }

    /**
     * Invalidating the authorization token
     *
     * You can ask that the server generates a new authorization token. This will invalidate any existing token for the
     * user.
     */
    @Test
    @Documented
    public void regenerate_token() throws PropertyValueException, IOException
    {
        // Given
        startServerWithConfiguredUser();
        String token = HTTP.POST( authURL(), HTTP.RawPayload.quotedJson( "{'username':'neo4j','password':'secret'}" ) )
                .get( "authorization_token" ).asText();

        // Document
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson( "{'password':'secret'}" ) )
                .post( server.baseUri().resolve( "/user/neo4j/authorization_token" ).toString() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        String newToken = data.get( "authorization_token" ).asText();
        assertThat( newToken, not( equalTo( token ) ));
        assertThat( newToken.length(), not(0));

        // And then the token I got back should work
        assertEquals(200, HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( newToken ) ).GET( authURL() ).status());

        // And then the old token should not be invalid
        assertEquals(401, HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( token ) ).GET( authURL() ).status());
    }

    /**
     * Changing your password
     *
     * Given that you know the current password, you can ask the server to change a users password. You can choose any
     * password you like, as long as it is different from the current password.
     */
    @Test
    @Documented
    public void change_password() throws PropertyValueException, IOException
    {
        // Given
        startServerWithConfiguredUser();
        String originalToken = HTTP.POST( authURL(), HTTP.RawPayload.quotedJson( "{'username':'neo4j','password':'secret'}" ) )
                .get( "authorization_token" ).asText();

        // Document
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson( "{'password':'secret', 'new_password':'qwerty'}" ) )
                .post( server.baseUri().resolve( "/user/neo4j/password" ).toString() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        String newToken = data.get( "authorization_token" ).asText();
        assertThat( newToken, equalTo( originalToken ));
        assertThat( newToken.length(), not( 0 ) );

        // And then the new password should work
        assertEquals(200, HTTP.POST( authURL(), HTTP.RawPayload.quotedJson( "{'username':'neo4j','password':'qwerty'}" ) ).status());

        // And then the old password should not be invalid
        assertEquals(422, HTTP.POST( authURL(), HTTP.RawPayload.quotedJson( "{'username':'neo4j','password':'secret'}" ) ).status());
    }

    /**
     * Setting authorization token
     *
     * In some cases you may want to explicitly set an authorization token for a user, for instance if you want to be
     * able to use the same authorization token for multiple Neo4j instances in a cluster.
     *
     * This can be done by taking an authorization token generated by Neo4j and asking other Neo4j instance to use that
     * token. This is similar to invalidating an existing token, except the new token to be used is passed in explicitly.
     */
    @Test
    @Documented
    public void set_token() throws PropertyValueException, IOException
    {
        // Given
        startServerWithConfiguredUser();

        String originalToken = HTTP.POST( authURL(), HTTP.RawPayload.quotedJson( "{'username':'neo4j','password':'secret'}" ) )
                .get( "authorization_token" ).asText();

        // Document
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson( "{'password':'secret', 'new_authorization_token':'EEB9E6883A24CEF7899CF35AD49D5944'}" ) )
                .post( server.baseUri().resolve( "/user/neo4j/authorization_token" ).toString() );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        String newToken = data.get( "authorization_token" ).asText();
        assertThat( newToken, equalTo( "EEB9E6883A24CEF7899CF35AD49D5944" ));

        // And then the token I got back should work
        assertEquals(200, HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( newToken ) ).GET( authURL() ).status());

        // And then the old token should not be invalid
        assertEquals( 401, HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( originalToken ) ).GET(
                authURL() ).status() );
    }

    @Test
    public void cantChangeToCurrentPassword() throws Exception
    {
        // Given
        startServer( true );

        // When
        HTTP.Response res = HTTP.POST( server.baseUri().resolve( "/user/neo4j/password" ).toString(),
                HTTP.RawPayload.quotedJson( "{'password':'neo4j', 'new_password':'neo4j'}" ) );

        // Then
        assertThat(res.status(), equalTo(422));
    }

    @Test
    public void shouldRateLimit() throws Exception
    {
        // Given
        startServer( true );
        assertRateLimited( "POST", server.baseUri().resolve( "/user/neo4j/password" ), "{'password':'something that is wrong', 'new_password':'secret'}");
        assertRateLimited( "POST", server.baseUri().resolve( "/user/neo4j/authorization_token" ), "{'password':'something that is wrong'}" );
        assertRateLimited( "POST", server.baseUri().resolve( "/user/neo4j/authorization_token" ), "{'password':'something that is wrong','new_authorization_token':'asd'}" );
    }

    @Test
    public void shouldRequireAuthorization() throws Exception
    {
        // Given
        startServer( true );
        assertAuthorizationNeeded( "POST", server.baseUri().resolve( "/user/neo4j/password" ), "{'password':'something that is wrong', 'new_password':'secret'}" );
        assertAuthorizationNeeded( "POST", server.baseUri().resolve( "/user/neo4j/authorization_token" ), "{'password':'something that is wrong'}" );
        assertAuthorizationNeeded( "POST", server.baseUri().resolve( "/user/neo4j/authorization_token" ), "{'password':'something that is wrong','new_authorization_token':'asd'}" );
    }

    private void assertAuthorizationNeeded( String method, URI ur, String payload ) throws JsonParseException
    {
        // When
        HTTP.Response response = HTTP.request( method, ur.toString(),
                HTTP.RawPayload.quotedJson( payload ) );

        // Then
        assertThat(response.status(), equalTo(422));
        assertThat(response.get("errors").get(0).get("code").asText(), equalTo("Neo.ClientError.Security.AuthenticationFailed"));
        assertThat( response.get( "errors" ).get( 0 ).get( "message" ).asText(), equalTo( "Invalid username and/or password." ) );
    }

    private void assertRateLimited( String method, URI uri, String payload ) throws JsonParseException
    {
        long timeout = System.currentTimeMillis() + 30_000;

        // When
        HTTP.Response response = null;
        while(System.currentTimeMillis() < timeout)
        {
            // Done in a loop because we're racing with the clock to get enough failed requests into 5 seconds
            response = HTTP.request( method, uri.toString(),
                    HTTP.RawPayload.quotedJson( payload ) );

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

    @After
    public void cleanup()
    {
        if(server != null) {server.stop();}
    }

    public void startServer(boolean authEnabled) throws IOException
    {
        new File( "neo4j-home/data/dbms/authorization" ).delete(); // TODO: Implement a common component for managing Neo4j file structure and use that here
        server = CommunityServerBuilder.server().withProperty( ServerSettings.authorization_enabled.name(),
                Boolean.toString( authEnabled ) ).build();
        server.start();
    }

    public void startServerWithConfiguredUser() throws IOException
    {
        startServer( true );
        // Set the password
        HTTP.Response put = HTTP.POST( server.baseUri().resolve( "/user/neo4j/password" ).toString(),
                HTTP.RawPayload.quotedJson( "{'password':'neo4j', 'new_password':'secret'}" ) );
        assertEquals( 200, put.status() );
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
