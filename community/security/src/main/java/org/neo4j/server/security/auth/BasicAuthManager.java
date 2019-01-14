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
package org.neo4j.server.security.auth;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.PasswordPolicy;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;
import org.neo4j.string.UTF8;

import static org.neo4j.kernel.api.security.AuthToken.invalidToken;

/**
 * Manages server authentication and authorization.
 * <p>
 * Through the BasicAuthManager you can create, update and delete userRepository, and authenticate using credentials.
 * <p>
 * NOTE: AuthManager will manage the lifecycle of the given UserRepository,
 *       so the given UserRepository should not be added to another LifeSupport.
 * </p>
 */
public class BasicAuthManager implements AuthManager, UserManager, UserManagerSupplier
{
    protected final AuthenticationStrategy authStrategy;
    protected final UserRepository userRepository;
    protected final PasswordPolicy passwordPolicy;
    private final UserRepository initialUserRepository;

    public BasicAuthManager( UserRepository userRepository, PasswordPolicy passwordPolicy,
            AuthenticationStrategy authStrategy, UserRepository initialUserRepository )
    {
        this.userRepository = userRepository;
        this.passwordPolicy = passwordPolicy;
        this.authStrategy = authStrategy;
        this.initialUserRepository = initialUserRepository;
    }

    public BasicAuthManager( UserRepository userRepository, PasswordPolicy passwordPolicy, Clock clock,
            UserRepository initialUserRepository, Config config )
    {
        this( userRepository, passwordPolicy, createAuthenticationStrategy( clock, config ), initialUserRepository );
    }

    @Override
    public void init() throws Throwable
    {
        userRepository.init();
        initialUserRepository.init();
    }

    @Override
    public void start() throws Throwable
    {
        userRepository.start();
        initialUserRepository.start();

        if ( userRepository.numberOfUsers() == 0 )
        {
            User neo4j = newUser( INITIAL_USER_NAME, UTF8.encode( INITIAL_PASSWORD ), true );
            if ( initialUserRepository.numberOfUsers() > 0 )
            {
                User user = initialUserRepository.getUserByName( INITIAL_USER_NAME );
                if ( user != null )
                {
                    userRepository.update( neo4j, user );
                }
            }
        }
    }

    @Override
    public void stop() throws Throwable
    {
        userRepository.stop();
        initialUserRepository.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        userRepository.shutdown();
        initialUserRepository.shutdown();
    }

    @Override
    public LoginContext login( Map<String,Object> authToken ) throws InvalidAuthTokenException
    {
        try
        {
            assertValidScheme( authToken );

            String username = AuthToken.safeCast( AuthToken.PRINCIPAL, authToken );
            byte[] password = AuthToken.safeCastCredentials( AuthToken.CREDENTIALS, authToken );

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
            return new BasicLoginContext( user, result );
        }
        finally
        {
            AuthToken.clearCredentials( authToken );
        }
    }

    @Override
    public User newUser( String username, byte[] initialPassword, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        try
        {
            userRepository.assertValidUsername( username );

            passwordPolicy.validatePassword( initialPassword );

            User user = new User.Builder()
                    .withName( username )
                    .withCredentials( LegacyCredential.forPassword( initialPassword ) )
                    .withRequiredPasswordChange( requirePasswordChange )
                    .build();
            userRepository.create( user );

            return user;
        }
        finally
        {
            // Clear password
            if ( initialPassword != null )
            {
                Arrays.fill( initialPassword, (byte) 0 );
            }
        }
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

    @Override
    public User silentlyGetUser( String username )
    {
        return userRepository.getUserByName( username );
    }

    @Override
    public void setUserPassword( String username, byte[] password, boolean requirePasswordChange )
            throws IOException, InvalidArgumentsException
    {
        try
        {
            User existingUser = getUser( username );

            passwordPolicy.validatePassword( password );

            if ( existingUser.credentials().matchesPassword( password ) )
            {
                throw new InvalidArgumentsException( "Old password and new password cannot be the same." );
            }

            try
            {
                User updatedUser = existingUser.augment().withCredentials( LegacyCredential.forPassword( password ) ).withRequiredPasswordChange(
                        requirePasswordChange ).build();
                userRepository.update( existingUser, updatedUser );
            }
            catch ( ConcurrentModificationException e )
            {
                // try again
                setUserPassword( username, password, requirePasswordChange );
            }
        }
        finally
        {
            // Clear password
            if ( password != null )
            {
                Arrays.fill( password, (byte) 0 );
            }
        }
    }

    @Override
    public Set<String> getAllUsernames()
    {
        return userRepository.getAllUsernames();
    }

    @Override
    public UserManager getUserManager( AuthSubject authSubject, boolean isUserManager )
    {
        return this;
    }

    @Override
    public UserManager getUserManager()
    {
        return this;
    }

    private void assertValidScheme( Map<String,Object> token ) throws InvalidAuthTokenException
    {
        String scheme = AuthToken.safeCast( AuthToken.SCHEME_KEY, token );
        if ( scheme.equals( "none" ) )
        {
            throw invalidToken( ", scheme 'none' is only allowed when auth is disabled." );
        }
        if ( !scheme.equals( AuthToken.BASIC_SCHEME ) )
        {
            throw invalidToken( ", scheme '" + scheme + "' is not supported." );
        }
    }

    private static AuthenticationStrategy createAuthenticationStrategy( Clock clock, Config config )
    {
        return new RateLimitedAuthenticationStrategy( clock, config );
    }
}
