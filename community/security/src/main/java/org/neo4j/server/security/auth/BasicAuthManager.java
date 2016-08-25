/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.security.auth;

import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

/**
 * Manages server authentication and authorization.
 * <p>
 * Through the BasicAuthManager you can create, update and delete userRepository, and authenticate using credentials.
 * <p>>
 * NOTE: AuthManager will manage the lifecycle of the given UserRepository,
 *       so the given UserRepository should not be added to another LifeSupport.
 * </p>
 */
public class BasicAuthManager implements AuthManager, UserManager, UserManagerSupplier
{
    protected final AuthenticationStrategy authStrategy;
    protected final UserRepository userRepository;
    protected final PasswordPolicy passwordPolicy;

    public BasicAuthManager( UserRepository userRepository, PasswordPolicy passwordPolicy, AuthenticationStrategy authStrategy )
    {
        this.userRepository = userRepository;
        this.passwordPolicy = passwordPolicy;
        this.authStrategy = authStrategy;
    }

    public BasicAuthManager( UserRepository userRepository, PasswordPolicy passwordPolicy, Clock clock )
    {
        this( userRepository, passwordPolicy, new RateLimitedAuthenticationStrategy( clock, 3 ) );
    }

    @Override
    public void init() throws Throwable
    {
        userRepository.init();
    }

    @Override
    public void start() throws Throwable
    {
        userRepository.start();

        if ( userRepository.numberOfUsers() == 0 )
        {
            newUser( "neo4j", "neo4j", true );
        }
    }

    @Override
    public void stop() throws Throwable
    {
        userRepository.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        userRepository.shutdown();
    }

    @Override
    public BasicAuthSubject login( Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        String username = AuthToken.safeCast( AuthToken.PRINCIPAL, authToken );
        String password = AuthToken.safeCast( AuthToken.CREDENTIALS, authToken );

        User user = userRepository.getUserByName( username );
        AuthenticationResult result = AuthenticationResult.FAILURE;
        if ( user != null )
        {
            result = authStrategy.authenticate( user, password );
            if ( result == AuthenticationResult.SUCCESS && user.passwordChangeRequired() )
            {
                result = AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
            }
        }
        return new BasicAuthSubject( this, user, result );
    }

    @Override
    public User newUser( String username, String initialPassword, boolean requirePasswordChange ) throws IOException,
            InvalidArgumentsException
    {
        assertValidUsername( username );

        passwordPolicy.validatePassword( initialPassword );

        User user = new User.Builder()
                .withName( username )
                .withCredentials( Credential.forPassword( initialPassword ) )
                .withRequiredPasswordChange( requirePasswordChange )
                .build();
        userRepository.create( user );

        return user;
    }

    @Override
    public boolean deleteUser( String username ) throws IOException, InvalidArgumentsException
    {
        User user = getUser( username );
        return user != null && userRepository.delete( user );
    }

    @Override
    public User getUser( String username ) throws InvalidArgumentsException
    {
        User user = userRepository.getUserByName( username );
        if ( user == null )
        {
            throw new InvalidArgumentsException( "User '" + username + "' does not exist." );
        }
        return user;
    }

    public void setPassword( AuthSubject authSubject, String username, String password ) throws IOException,
            InvalidArgumentsException
    {
        BasicAuthSubject basicAuthSubject = BasicAuthSubject.castOrFail( authSubject );

        if ( !basicAuthSubject.doesUsernameMatch( username ) )
        {
            throw new AuthorizationViolationException( "Invalid attempt to change the password for user " + username );
        }

        setUserPassword( username, password );
    }

    @Override
    public void setUserPassword( String username, String password ) throws IOException,
            InvalidArgumentsException
    {
        User existingUser = getUser( username );

        passwordPolicy.validatePassword( password );

        if ( existingUser.credentials().matchesPassword( password ) )
        {
            throw new InvalidArgumentsException( "Old password and new password cannot be the same." );
        }

        try
        {
            User updatedUser = existingUser.augment()
                    .withCredentials( Credential.forPassword( password ) )
                    .withRequiredPasswordChange( false )
                    .build();
            userRepository.update( existingUser, updatedUser );
        } catch ( ConcurrentModificationException e )
        {
            // try again
            setUserPassword( username, password );
        }
    }

    @Override
    public Set<String> getAllUsernames()
    {
        return userRepository.getAllUsernames();
    }

    private void assertValidUsername( String name ) throws InvalidArgumentsException
    {
        if ( name.isEmpty() )
        {
            throw new InvalidArgumentsException( "The provided user name is empty." );
        }
        if ( !userRepository.isValidUsername( name ) )
        {
            throw new InvalidArgumentsException(
                    "User name '" + name +
                            "' contains illegal characters. Use simple ascii characters and numbers." );
        }
    }

    @Override
    public UserManager getUserManager()
    {
        return this;
    }
}
