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
package org.neo4j.server.rest.dbms;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.server.security.auth.LegacyCredential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicAuthManager;
import org.neo4j.server.security.auth.BasicLoginContext;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.test.server.EntityOutputFormat;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class UserServiceTest
{
    protected static final User NEO4J_USER = new User.Builder( "neo4j", LegacyCredential.forPassword( "neo4j" ) )
            .withRequiredPasswordChange( true ).build();

    protected final PasswordPolicy passwordPolicy = new BasicPasswordPolicy();
    protected final UserRepository userRepository = new InMemoryUserRepository();

    protected UserManagerSupplier userManagerSupplier;
    protected LoginContext neo4jContext;
    protected Principal neo4jPrinciple;
    private HttpServletRequest request;

    protected void setupAuthManagerAndSubject()
    {

        userManagerSupplier = new BasicAuthManager( userRepository, passwordPolicy,
                mock( AuthenticationStrategy.class), new InMemoryUserRepository() );
        neo4jContext = new BasicLoginContext( NEO4J_USER, AuthenticationResult.SUCCESS );
    }

    @Before
    public void setUp() throws InvalidArgumentsException, IOException
    {
        request = mock( HttpServletRequest.class );
        userRepository.create( NEO4J_USER );
        setupAuthManagerAndSubject();
        neo4jPrinciple = new DelegatingPrincipal( "neo4j", neo4jContext );
    }

    @After
    public void tearDown() throws IOException
    {
        userRepository.delete( NEO4J_USER );
    }

    @Test
    public void shouldReturnValidUserRepresentation() throws Exception
    {
        // Given
        when( request.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.getUser( "neo4j", request );

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
        when( request.getUserPrincipal() ).thenReturn( null );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.getUser( "neo4j", request );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
    }

    @Test
    public void shouldReturn404WhenRequestingUserIfDifferentUser() throws Exception
    {
        // Given
        when( request.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( mock( BasicAuthManager.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.getUser( "fred", request );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
    }

    @Test
    public void shouldReturn404WhenRequestingUserIfUnknownUser() throws Exception
    {
        // Given
        when( request.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        userManagerSupplier.getUserManager().deleteUser( "neo4j" );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.getUser( "neo4j", request );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
    }

    @Test
    public void shouldChangePasswordAndReturnSuccess() throws Exception
    {
        // Given
        when( request.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", request, "{ \"password\" : \"test\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 200 ) );
        userManagerSupplier.getUserManager().getUser( "neo4j" ).credentials().matchesPassword( "test" );
    }

    @Test
    public void shouldReturn404WhenChangingPasswordIfNotAuthenticated() throws Exception
    {
        // Given
        when( request.getUserPrincipal() ).thenReturn( null );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( mock( BasicAuthManager.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", request, "{ \"password\" : \"test\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
    }

    @Test
    public void shouldReturn404WhenChangingPasswordIfDifferentUser() throws Exception
    {
        // Given
        when( request.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        UserManager userManager = mock( UserManager.class );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "fred", request, "{ \"password\" : \"test\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 404 ) );
        verifyZeroInteractions( userManager );
    }

    @Test
    public void shouldReturn422WhenChangingPasswordIfUnknownUser() throws Exception
    {
        // Given
        when( request.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        userRepository.delete( NEO4J_USER );

        // When
        Response response = userService.setPassword( "neo4j", request, "{ \"password\" : \"test\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 422 ) );
    }

    @Test
    public void shouldReturn400IfPayloadIsInvalid() throws Exception
    {
        // Given
        when( request.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( mock( BasicAuthManager.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", request, "xxx" );

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
        when( request.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( mock( BasicAuthManager.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", request, "{ \"unknown\" : \"unknown\" }" );

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
        when( request.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( mock( BasicAuthManager.class ), new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", request, "{ \"password\" : 1 }" );

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
        when( request.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", request, "{ \"password\" : \"\" }" );

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
        when( request.getUserPrincipal() ).thenReturn( neo4jPrinciple );

        OutputFormat outputFormat = new EntityOutputFormat( new JsonFormat(), new URI( "http://www.example.com" ), null );
        UserService userService = new UserService( userManagerSupplier, new JsonFormat(), outputFormat );

        // When
        Response response = userService.setPassword( "neo4j", request, "{ \"password\" : \"neo4j\" }" );

        // Then
        assertThat( response.getStatus(), equalTo( 422 ) );
        String json = new String( (byte[]) response.getEntity() );
        assertNotNull( json );
        assertThat( json, containsString( "\"code\" : \"Neo.ClientError.General.InvalidArguments\"" ) );
        assertThat( json, containsString( "\"message\" : \"Old password and new password cannot be the same.\"" ) );
    }
}
