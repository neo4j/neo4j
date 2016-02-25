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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class UsersDocIT extends ExclusiveServerTestBase
{
    public @Rule TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );
    private CommunityNeoServer server;

    @Before
    public void setUp()
    {
        gen.get().setSection( "dev/rest-api" );
    }

    @Test
    @Documented( "User status\n" +
                 "\n" +
                 "Given that you know the current password, you can ask the server for the user status." )
    public void user_status() throws JsonParseException, IOException
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
    @Documented( "User status on first access\n" +
                 "\n" +
                 "On first access, and using the default password, the user status will indicate that the users password requires changing." )
    public void user_status_first_access() throws JsonParseException, IOException
    {
        // Given
        startServer( true );

        // Document
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .withHeader( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "neo4j" ) )
                .get( userURL( "neo4j" ) );

        // Then
        JsonNode data = JsonHelper.jsonNode( response.entity() );
        assertThat( data.get( "username" ).asText(), equalTo( "neo4j" ) );
        assertThat( data.get( "password_change_required" ).asBoolean(), equalTo( true ) );
        assertThat( data.get( "password_change" ).asText(), equalTo( passwordURL( "neo4j" ) ) );
    }

    @Test
    @Documented( "Changing the user password\n" +
                 "\n" +
                 "Given that you know the current password, you can ask the server to change a users password. You can choose any\n" +
                 "password you like, as long as it is different from the current password." )
    public void change_password() throws JsonParseException, IOException
    {
        // Given
        startServer( true );

        // Document
        RESTDocsGenerator.ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .withHeader( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "neo4j" ) )
                .payload( quotedJson( "{'password':'secret'}" ) )
                .post( server.baseUri().resolve( "/user/neo4j/password" ).toString() );

        // Then the new password should work
        assertEquals( 200, HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "secret" ) ).GET( dataURL() ).status() );

        // Then the old password should not be invalid
        assertEquals( 401, HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "neo4j" ) ).POST( dataURL() ).status() );
    }

    @Test
    public void cantChangeToCurrentPassword() throws Exception
    {
        // Given
        startServer( true );

        // When
        HTTP.Response res = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "neo4j" ) ).POST(
                server.baseUri().resolve( "/user/neo4j/password" ).toString(),
                HTTP.RawPayload.quotedJson( "{'password':'neo4j'}" ) );

        // Then
        assertThat( res.status(), equalTo( 422 ) );
    }

    @After
    public void cleanup()
    {
        if(server != null) {server.stop();}
    }

    public void startServer(boolean authEnabled) throws IOException
    {
        File file = ServerTestUtils.getRelativeFile( ServerSettings.auth_store );
        FileUtils.deleteFile( file );
        server = CommunityServerBuilder.server()
                .withProperty( ServerSettings.auth_enabled.name(), Boolean.toString( authEnabled ) )
                .withProperty( ServerSettings.auth_store.name(), file.getAbsolutePath() )
                .build();
        server.start();
    }

    public void startServerWithConfiguredUser() throws IOException
    {
        startServer( true );
        // Set the password
        HTTP.Response post = HTTP.withHeaders( HttpHeaders.AUTHORIZATION, challengeResponse( "neo4j", "neo4j" ) ).POST(
                server.baseUri().resolve( "/user/neo4j/password" ).toString(),
                HTTP.RawPayload.quotedJson( "{'password':'secret'}" )
        );
        assertEquals( 200, post.status() );
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

    private String quotedJson( String singleQuoted )
    {
        return singleQuoted.replaceAll( "'", "\"" );
    }
}
