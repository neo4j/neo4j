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
package org.neo4j.server.rest.dbms;

import org.junit.Test;

import java.net.URI;
import java.security.Principal;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.security.auth.AuthManager;
import org.neo4j.server.security.auth.Credential;
import org.neo4j.server.security.auth.User;
import org.neo4j.test.server.EntityOutputFormat;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class UserServiceTest
{
    private static final Principal NEO4J_PRINCIPLE = new Principal()
    {
        @Override
        public String getName()
        {
            return "neo4j";
        }
    };
    private static final User NEO4J_USER = new User( "neo4j", Credential.forPassword( "neo4j" ), true );

    @Test
    public void shouldReturnValidUserRepresentation() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( NEO4J_PRINCIPLE );

        AuthManager authManager = mock( AuthManager.class );
        when( authManager.getUser( "neo4j" ) ).thenReturn( NEO4J_USER );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( authManager, new JsonFormat(), outputFormat );

        // When
        Response response = userService.getUser( "neo4j", req );

        // Then
        assertThat( response.getStatus(), equalTo( 200 ) );
        String json = new String( (byte[]) response.getEntity() );
        assertNotNull( json );
        assertThat( json, containsString( "\"username\" : \"neo4j\"" ) );
        assertThat( json, containsString( "\"password_change\" : \"http://www.example.com/user/neo4j/password\"" ) );
        assertThat( json, containsString( "\"password_change_required\" : true" ) );
    }

    @Test
    public void shouldReturn404WhenRequestingUserIfNotAuthenticated() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( NEO4J_PRINCIPLE );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( mock( AuthManager.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.getUser( "neo4j", req );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
    }

    @Test
    public void shouldReturn404WhenRequestingUserIfDifferentUser() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( NEO4J_PRINCIPLE );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( mock( AuthManager.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.getUser( "fred", req );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
    }

    @Test
    public void shouldReturn404WhenRequestingUserIfUnknownUser() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( NEO4J_PRINCIPLE );

        AuthManager authManager = mock( AuthManager.class );
        when( authManager.getUser( "neo4j" ) ).thenReturn( null );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( authManager, new JsonFormat(), outputFormat );

        // When
        Response response = userService.getUser( "neo4j", req );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
    }

    @Test
    public void shouldChangePasswordAndReturnSuccess() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( NEO4J_PRINCIPLE );

        AuthManager authManager = mock( AuthManager.class );
        when( authManager.getUser( "neo4j" ) ).thenReturn( NEO4J_USER );
        when( authManager.setPassword( "neo4j", "test" ) ).thenReturn( NEO4J_USER );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( authManager, new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", req, "{ \"password\" : \"test\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 200 ) );
        verify( authManager ).setPassword( "neo4j", "test" );
    }

    @Test
    public void shouldReturn404WhenChangingPasswordIfNotAuthenticated() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( null );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( mock( AuthManager.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", req, "{ \"password\" : \"test\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
    }

    @Test
    public void shouldReturn404WhenChangingPasswordIfDifferentUser() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( NEO4J_PRINCIPLE );

        AuthManager authManager = mock( AuthManager.class );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( authManager, new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "fred", req, "{ \"password\" : \"test\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
        verifyZeroInteractions( authManager );
    }

    @Test
    public void shouldReturn404WhenChangingPasswordIfUnknownUser() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( NEO4J_PRINCIPLE );

        AuthManager authManager = mock( AuthManager.class );
        when( authManager.getUser( "neo4j" ) ).thenReturn( null );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( authManager, new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", req, "{ \"password\" : \"test\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
        verify( authManager ).getUser( "neo4j" );
        verifyNoMoreInteractions( authManager );
    }

    @Test
    public void shouldReturn400IfPayloadIsInvalid() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( NEO4J_PRINCIPLE );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( mock( AuthManager.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", req, "xxx" );

        // Then
        assertThat( response.getStatus(), equalTo( 400 ) );
        String json = new String( (byte[]) response.getEntity() );
        assertNotNull( json );
        assertThat( json, containsString( "\"code\" : \"Neo.ClientError.Request.InvalidFormat\"" ) );
    }

    @Test
    public void shouldReturn422IfMissingPassword() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( NEO4J_PRINCIPLE );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( mock( AuthManager.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", req, "{ \"unknown\" : \"unknown\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 422 ) );
        String json = new String( (byte[]) response.getEntity() );
        assertNotNull( json );
        assertThat( json, containsString( "\"code\" : \"Neo.ClientError.Request.InvalidFormat\"" ) );
        assertThat( json, containsString( "\"message\" : \"Required parameter 'password' is missing.\"" ) );
    }

    @Test
    public void shouldReturn422IfInvalidPasswordType() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( NEO4J_PRINCIPLE );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( mock( AuthManager.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", req, "{ \"password\" : 1 }" );

        // Then
        assertThat( response.getStatus(), equalTo( 422 ) );
        String json = new String( (byte[]) response.getEntity() );
        assertNotNull( json );
        assertThat( json, containsString( "\"code\" : \"Neo.ClientError.Request.InvalidFormat\"" ) );
        assertThat( json, containsString( "\"message\" : \"Expected 'password' to be a string.\"" ) );
    }

    @Test
    public void shouldReturn422IfEmptyPassword() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( NEO4J_PRINCIPLE );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( mock( AuthManager.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", req, "{ \"password\" : \"\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 422 ) );
        String json = new String( (byte[]) response.getEntity() );
        assertNotNull( json );
        assertThat( json, containsString( "\"code\" : \"Neo.ClientError.Request.Invalid\"" ) );
        assertThat( json, containsString( "\"message\" : \"Password cannot be empty.\"" ) );
    }

    @Test
    public void shouldReturn422IfPasswordIdentical() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( NEO4J_PRINCIPLE );

        AuthManager authManager = mock( AuthManager.class );
        when( authManager.getUser( "neo4j" ) ).thenReturn( NEO4J_USER );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( authManager, new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", req, "{ \"password\" : \"neo4j\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 422 ) );
        String json = new String( (byte[]) response.getEntity() );
        assertNotNull( json );
        assertThat( json, containsString( "\"code\" : \"Neo.ClientError.Request.Invalid\"" ) );
        assertThat( json, containsString( "\"message\" : \"Old password and new password cannot be the same.\"" ) );
    }
}
