/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.helpers.Clock;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;
import org.neo4j.server.security.auth.exception.IllegalUsernameException;

/**
 * Manages server authentication and authorization.
 * <p/>
 * Through the AuthManager you can create, update and delete users, and authenticate using credentials.
 */
public class AuthManager extends LifecycleAdapter
{
    private final AuthenticationStrategy authStrategy;
    private final UserRepository users;

    public AuthManager( UserRepository users, AuthenticationStrategy authStrategy )
    {
        this.users = users;
        this.authStrategy = authStrategy;
    }

    public AuthManager( Clock clock, UserRepository users )
    {
        this( users, new RateLimitedAuthenticationStrategy( clock, 3 ) );
    }

    @Override
    public void start() throws Throwable
    {
        if ( users.numberOfUsers() == 0 )
        {
            newUser( "neo4j", "neo4j", true );
        }
    }

    public AuthenticationResult authenticate( String username, String password )
    {
        User user = users.findByName( username );
        if ( user == null )
        {
            return AuthenticationResult.FAILURE;
        }
        AuthenticationResult result = authStrategy.authenticate( user, password );
        if ( result != AuthenticationResult.SUCCESS )
        {
            return result;
        }
        if ( user.passwordChangeRequired() )
        {
            return AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
        }
        return AuthenticationResult.SUCCESS;
    }

    public User newUser( String username, String initialPassword, boolean requirePasswordChange ) throws IOException, IllegalUsernameException
    {
        assertValidName( username );
        User user = new User.Builder()
                .withName( username )
                .withCredentials( Credential.forPassword( initialPassword ) )
                .withRequiredPasswordChange( requirePasswordChange )
                .build();
        users.create( user );
        return user;
    }

    public boolean deleteUser( String username ) throws IOException
    {
        User user = users.findByName( username );
        return user != null && users.delete( user );
    }

    public User getUser( String username )
    {
        return users.findByName( username );
    }

    public User setPassword( String username, String password ) throws IOException
    {
        User existingUser = users.findByName( username );
        if ( existingUser == null )
        {
            return null;
        }

        if ( existingUser.credentials().matchesPassword( password ) )
        {
            return existingUser;
        }

        try
        {
            User updatedUser = existingUser.augment()
                    .withCredentials( Credential.forPassword( password ) )
                    .withRequiredPasswordChange( false )
                    .build();
            users.update( existingUser, updatedUser );
            return updatedUser;
        } catch ( ConcurrentModificationException e )
        {
            // try again
            return setPassword( username, password );
        }
    }

    private void assertValidName( String name )
    {
        if ( !users.isValidName( name ) )
        {
            throw new IllegalArgumentException( "User name contains illegal characters. Please use simple ascii characters and numbers." );
        }
    }
}
