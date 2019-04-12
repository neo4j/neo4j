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
package org.neo4j.server.rest.dbms;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.security.auth.BasicLoginContext;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.test.server.EntityOutputFormat;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;
import static org.neo4j.server.security.auth.SecurityTestUtils.simpleBasicSystemGraphRealm;

public class UserServiceTest
{
    protected static final String USERNAME = "neo";

    private UserManagerSupplier userManagerSupplier;
    private Principal neo4jPrinciple;
    private UserManager userManager;

    protected UserManagerSupplier setupUserManagerSupplier()
    {
        return simpleBasicSystemGraphRealm( Config.defaults() );
    }

    protected LoginContext setupSubject() throws InvalidArgumentsException
    {
        return new BasicLoginContext( userManager.getUser( USERNAME ), AuthenticationResult.SUCCESS );
    }

    @Before
    public void setUp() throws InvalidArgumentsException, IOException
    {
        userManagerSupplier = setupUserManagerSupplier();
        userManager = userManagerSupplier.getUserManager();
        userManager.newUser( USERNAME, password( "neo4j" ), true );
        LoginContext subject = setupSubject();

        neo4jPrinciple = new DelegatingPrincipal( USERNAME, subject );
    }

    @After
    public void tearDown() throws IOException, InvalidArgumentsException
    {
        if ( userManager.silentlyGetUser( USERNAME ) != null )
        {
            userManager.deleteUser( USERNAME );
        }
    }

    @Test
    public void shouldReturnValidUserRepresentation() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.getUser( USERNAME, req );

        // Then
        assertThat( response.getStatus(), equalTo( 200 ) );
        String json = new String( (byte[]) response.getEntity() );
        assertNotNull( json );
        assertThat( json, containsString( String.format( "\"username\" : \"%s\"", USERNAME ) ) );
        assertThat( json, containsString( String.format( "\"password_change\" : \"http://www.example.com/user/%s/password\"", USERNAME ) ) );
        assertThat( json, containsString( "\"password_change_required\" : true" ) );
    }

    @Test
    public void shouldReturn404WhenRequestingUserIfNotAuthenticated() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( null );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.getUser( USERNAME, req );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
    }

    @Test
    public void shouldReturn404WhenRequestingUserIfDifferentUser() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( mock( BasicSystemGraphRealm.class ), new JsonFormat(), outputFormat );

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
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        userManager.deleteUser( USERNAME );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.getUser( USERNAME, req );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
    }

    @Test
    public void shouldChangePasswordAndReturnSuccess() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( USERNAME, req, "{ \"password\" : \"test\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 200 ) );
        userManager.getUser( USERNAME ).credentials().matchesPassword( password( "test" ) );
    }

    @Test
    public void shouldReturn404WhenChangingPasswordIfNotAuthenticated() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( null );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( mock( BasicSystemGraphRealm.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( USERNAME, req, "{ \"password\" : \"test\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
    }

    @Test
    public void shouldReturn404WhenChangingPasswordIfDifferentUser() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        UserManager userManager = mock( UserManager.class );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "fred", req, "{ \"password\" : \"test\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
        verifyZeroInteractions( userManager );
    }

    @Test
    public void shouldReturn422WhenChangingPasswordIfUnknownUser() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        userManager.deleteUser( USERNAME );

        // When
        Response response = userService.setPassword( USERNAME, req, "{ \"password\" : \"test\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 422 ) );
    }

    @Test
    public void shouldReturn400IfPayloadIsInvalid() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( mock( BasicSystemGraphRealm.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( USERNAME, req, "xxx" );

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
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( mock( BasicSystemGraphRealm.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( USERNAME, req, "{ \"unknown\" : \"unknown\" }" );

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
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( mock( BasicSystemGraphRealm.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( USERNAME, req, "{ \"password\" : 1 }" );

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
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( USERNAME, req, "{ \"password\" : \"\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 422 ) );
        String json = new String( (byte[]) response.getEntity() );
        assertNotNull( json );
        assertThat( json, containsString( "\"code\" : \"Neo.ClientError.General.InvalidArguments\"" ) );
        assertThat( json, containsString( "\"message\" : \"A password cannot be empty.\"" ) );
    }

    @Test
    public void shouldReturn422IfPasswordIdentical() throws Exception
    {
        // Given
        HttpServletRequest req = mock( HttpServletRequest.class );
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ) );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( USERNAME, req, "{ \"password\" : \"neo4j\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 422 ) );
        String json = new String( (byte[]) response.getEntity() );
        assertNotNull( json );
        assertThat( json, containsString( "\"code\" : \"Neo.ClientError.General.InvalidArguments\"" ) );
        assertThat( json, containsString( "\"message\" : \"Old password and new password cannot be the same.\"" ) );
    }
}
