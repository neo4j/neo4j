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

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.server.http.error.Neo4jHttpExceptionMapper;
import org.neo4j.server.rest.TestWebServer;
import org.neo4j.server.rest.repr.PasswordChangeRepresentation;
import org.neo4j.server.security.auth.BasicLoginContext;
import org.neo4j.test.server.HTTP;

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
    private UriInfo uriInfo;
    private UriBuilder uriBuilder;
    private HttpServletRequest req;
    private String userResourceUri;

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

        req = mock( HttpServletRequest.class );

        uriInfo = mock( UriInfo.class );
        uriBuilder = mock( UriBuilder.class );
        when( uriInfo.getAbsolutePathBuilder() ).thenReturn( uriBuilder );
        when( uriBuilder.path( "password" ) ).thenReturn( uriBuilder );

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
    public void shouldReturnValidUserRepresentation()
    {
        // Given
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );
        when( uriBuilder.build() ).thenReturn( URI.create( String.format( "http://www.example.com/user/%s/password", USERNAME ) ) );

        UserService userService = new UserService( userManagerSupplier, req, uriInfo );

        doWithServer( userService, () ->
        {
            // When
            HTTP.Response response = HTTP.GET( userResourceUri + "/" + USERNAME );

            // Then
            assertThat( response.status(), equalTo( 200 ) );
            String json = response.rawContent();
            assertNotNull( json );
            assertThat( json, containsString( String.format( "\"username\":\"%s\"", USERNAME ) ) );
            assertThat( json, containsString( String.format( "\"password_change\":\"http://www.example.com/user/%s/password\"", USERNAME ) ) );
            assertThat( json, containsString( "\"password_change_required\":true" ) );
        } );
    }

    @Test
    public void shouldReturn404WhenRequestingUserIfNotAuthenticated()
    {
        // Given
        when( req.getUserPrincipal() ).thenReturn( null );
        when( uriBuilder.build() ).thenReturn( URI.create( String.format( "http://www.example.com/user/%s/password", USERNAME ) ) );

        UserService userService = new UserService( userManagerSupplier, req, uriInfo );

        doWithServer( userService, () ->
        {
            // When
            HTTP.Response response = HTTP.GET( userResourceUri + "/" + USERNAME );

            // Then
            assertThat( response.status(), equalTo( 404 ) );
        } );
    }

    @Test
    public void shouldReturn404WhenRequestingUserIfDifferentUser()
    {
        // Given
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );
        when( uriBuilder.build() ).thenReturn( URI.create( String.format( "http://www.example.com/user/%s/password", USERNAME ) ) );

        UserService userService = new UserService( userManagerSupplier, req, uriInfo );

        doWithServer( userService, () ->
        {
            // When
            HTTP.Response response = HTTP.GET( userResourceUri + "/fred" );

            // Then
            assertThat( response.status(), equalTo( 404 ) );
        } );
    }

    @Test
    public void shouldReturn404WhenRequestingUserIfUnknownUser() throws Exception
    {
        // Given
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );
        when( uriBuilder.build() ).thenReturn( URI.create( String.format( "http://www.example.com/user/%s/password", USERNAME ) ) );

        userManager.deleteUser( USERNAME );

        UserService userService = new UserService( userManagerSupplier, req, uriInfo );

        doWithServer( userService, () ->
        {
            // When
            HTTP.Response response = HTTP.GET( userResourceUri + "/fred" );

            // Then
            assertThat( response.status(), equalTo( 404 ) );
        } );
    }

    @Test
    public void shouldChangePasswordAndReturnSuccess() throws Exception
    {
        // Given
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );
        when( uriBuilder.build() ).thenReturn( URI.create( String.format( "http://www.example.com/user/%s/password", USERNAME ) ) );

        UserService userService = new UserService( userManagerSupplier, req, uriInfo );

        PasswordChangeRepresentation passwordChangeRepresentation = new PasswordChangeRepresentation();
        passwordChangeRepresentation.setPassword( "test" );

        doWithServer( userService, () ->
        {
            // When
            HTTP.Response response = HTTP.POST( userResourceUri + "/" + USERNAME + "/password", passwordChangeRepresentation );

            // Then
            assertThat( response.status(), equalTo( 204 ) );
        } );
        userManager.getUser( USERNAME ).credentials().matchesPassword( password( "test" ) );
    }

    @Test
    public void shouldReturn404WhenChangingPasswordIfNotAuthenticated()
    {
        // Given
        when( req.getUserPrincipal() ).thenReturn( null );
        when( uriBuilder.build() ).thenReturn( URI.create( String.format( "http://www.example.com/user/%s/password", USERNAME ) ) );

        UserService userService = new UserService( userManagerSupplier, req, uriInfo );

        PasswordChangeRepresentation passwordChangeRepresentation = new PasswordChangeRepresentation();
        passwordChangeRepresentation.setPassword( "test" );

        doWithServer( userService, () ->
        {
            // When
            HTTP.Response response = HTTP.POST( userResourceUri + "/" + USERNAME + "/password", passwordChangeRepresentation );

            // Then
            assertThat( response.status(), equalTo( 404 ) );
        } );
    }

    @Test
    public void shouldReturn404WhenChangingPasswordIfDifferentUser()
    {
        // Given
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );
        when( uriBuilder.build() ).thenReturn( URI.create( String.format( "http://www.example.com/user/%s/password", USERNAME ) ) );

        UserManager userManager = mock( UserManager.class );

        UserService userService = new UserService( userManagerSupplier, req, uriInfo );

        PasswordChangeRepresentation passwordChangeRepresentation = new PasswordChangeRepresentation();
        passwordChangeRepresentation.setPassword( "test" );

        doWithServer( userService, () ->
        {
            // When
            HTTP.Response response = HTTP.POST( userResourceUri + "/fred/password", passwordChangeRepresentation );

            // Then
            assertThat( response.status(), equalTo( 404 ) );
        } );

        verifyZeroInteractions( userManager );
    }

    @Test
    public void shouldReturn422WhenChangingPasswordIfUnknownUser() throws Exception
    {
        // Given
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );
        when( uriBuilder.build() ).thenReturn( URI.create( String.format( "http://www.example.com/user/%s/password", USERNAME ) ) );

        UserService userService = new UserService( userManagerSupplier, req, uriInfo );

        userManager.deleteUser( USERNAME );

        PasswordChangeRepresentation passwordChangeRepresentation = new PasswordChangeRepresentation();
        passwordChangeRepresentation.setPassword( "test" );

        doWithServer( userService, () ->
        {
            // When
            HTTP.Response response = HTTP.POST( userResourceUri + "/" + USERNAME + "/password", passwordChangeRepresentation );

            // Then
            assertThat( response.status(), equalTo( 422 ) );
        } );
    }

    @Test
    public void shouldReturn422IfMissingPassword()
    {
        // Given
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );
        when( uriBuilder.build() ).thenReturn( URI.create( String.format( "http://www.example.com/user/%s/password", USERNAME ) ) );

        UserService userService = new UserService( userManagerSupplier, req, uriInfo );

        PasswordChangeRepresentation passwordChangeRepresentation = new PasswordChangeRepresentation();
        passwordChangeRepresentation.setPassword( null );

        doWithServer( userService, () ->
        {
            // When
            HTTP.Response response = HTTP.POST( userResourceUri + "/" + USERNAME + "/password", passwordChangeRepresentation );

            // Then
            assertThat( response.status(), equalTo( 422 ) );
            String json = response.rawContent();
            assertNotNull( json );
            assertThat( json, containsString( "\"code\":\"Neo.ClientError.Request.InvalidFormat\"" ) );
            assertThat( json, containsString( "\"message\":\"Required parameter 'password' is missing.\"" ) );
        } );
    }

    @Test
    public void shouldReturn422IfEmptyPassword()
    {
        // Given
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );
        when( uriBuilder.build() ).thenReturn( URI.create( String.format( "http://www.example.com/user/%s/password", USERNAME ) ) );

        UserService userService = new UserService( userManagerSupplier, req, uriInfo );

        PasswordChangeRepresentation passwordChangeRepresentation = new PasswordChangeRepresentation();
        passwordChangeRepresentation.setPassword( "" );

        doWithServer( userService, () ->
        {
            // When
            HTTP.Response response = HTTP.POST( userResourceUri + "/" + USERNAME + "/password", passwordChangeRepresentation );

            // Then
            assertThat( response.status(), equalTo( 422 ) );
            String json = response.rawContent();
            assertNotNull( json );
            assertThat( json, containsString( "\"code\":\"Neo.ClientError.General.InvalidArguments\"" ) );
            assertThat( json, containsString( "\"message\":\"A password cannot be empty.\"" ) );
        } );
    }

    @Test
    public void shouldReturn422IfPasswordIdentical()
    {
        // Given
        when( req.getUserPrincipal() ).thenReturn( neo4jPrinciple );
        when( uriBuilder.build() ).thenReturn( URI.create( String.format( "http://www.example.com/user/%s/password", USERNAME ) ) );

        UserService userService = new UserService( userManagerSupplier, req, uriInfo );

        PasswordChangeRepresentation passwordChangeRepresentation = new PasswordChangeRepresentation();
        passwordChangeRepresentation.setPassword( "neo4j" );

        doWithServer( userService, () ->
        {
            // When
            HTTP.Response response = HTTP.POST( userResourceUri + "/" + USERNAME + "/password", passwordChangeRepresentation );

            // Then
            assertThat( response.status(), equalTo( 422 ) );
            String json = response.rawContent();
            assertNotNull( json );
            assertThat( json, containsString( "\"code\":\"Neo.ClientError.General.InvalidArguments\"" ) );
            assertThat( json, containsString( "\"message\":\"Old password and new password cannot be the same.\"" ) );
        } );
    }

    private void doWithServer( Object userEndpoint, Runnable code )
    {
        TestWebServer testWebServer = new TestWebServer( "/*", Arrays.asList( JacksonJsonProvider.class, Neo4jHttpExceptionMapper.class ),
                Collections.singletonList( userEndpoint ) );
        testWebServer.start();
        int port = testWebServer.getPort();
        userResourceUri = "http://localhost:" + port + "/user";
        try
        {
            code.run();
        }
        finally
        {
            testWebServer.stop();
        }
    }
}
