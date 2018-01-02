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

import com.sun.jersey.core.util.Base64;
import org.codehaus.jackson.JsonNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.server.rest.RESTDocsGenerator;
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
        FileUtils.deleteFile( new File( "neo4j-home/data/dbms/authorization" ) ); // TODO: Implement a common component for managing Neo4j file structure and use that here
        server = CommunityServerBuilder.server().withProperty( ServerSettings.auth_enabled.name(),
                Boolean.toString( authEnabled ) ).build();
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
        return new String( Base64.encode( value ), Charset
                .forName( "UTF-8" ));
    }

    private String quotedJson( String singleQuoted )
    {
        return singleQuoted.replaceAll( "'", "\"" );
    }
}
