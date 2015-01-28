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
import java.security.SecureRandom;

import org.neo4j.helpers.Clock;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.impl.util.BytePrinter;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;
import org.neo4j.server.security.auth.exception.IllegalTokenException;
import org.neo4j.server.security.auth.exception.IllegalUsernameException;
import org.neo4j.server.security.auth.exception.TooManyAuthenticationAttemptsException;

/**
 * Manages server authentication and authorization. This is mainly a coordinating component, it delegates to other
 * internal components for the majority of its work.
 *
 * Through the SecurityCentral you can create and edit users, check their credentials and their privileges.
 */
public class SecurityCentral extends LifecycleAdapter
{
    public static final User UNAUTHENTICATED = new UnauthenticatedUser();

    private final Authentication authentication;
    private final UserRepository users;
    private final SecureRandom rand = new SecureRandom();

    public SecurityCentral( Clock clock, UserRepository users )
    {
        this.users = users;
        this.authentication = new Authentication( clock, users, 3 );
    }

    @Override
    public void start() throws Throwable
    {
        if(users.numberOfUsers() == 0)
        {
            newUser( "neo4j", "neo4j", true, Privileges.ADMIN );
        }
    }

    /** Determine if a set of credentials are valid. */
    public boolean authenticate( String user, String password ) throws TooManyAuthenticationAttemptsException
    {
        return authentication.authenticate( user, password );
    }

    public User newUser( String name, String initialPassword, boolean requirePasswordChange, Privileges privileges ) throws IOException, IllegalUsernameException
    {
        try
        {
            assertValidName( name );
            User user = new User.Builder()
                    .withName( name )
                    .withToken( newToken() )
                    .withCredentials( authentication.createPasswordCredential( initialPassword ) )
                    .withRequiredPasswordChange( requirePasswordChange )
                    .withPrivileges( privileges )
                    .build();
            users.create( user );
            return user;
        } catch(IllegalTokenException e)
        {
            throw new ThisShouldNotHappenError( "Jake", "There is no token set at this point.", e );
        }
    }

    /**
     * Get a user, given a token. If no user is associated with the token, return an
     * {@link org.neo4j.server.security.auth.UnauthenticatedUser unauthenticated} user.
     */
    public User userForToken( String token )
    {
        if(token == null )
        {
            return UNAUTHENTICATED;
        }

        User user = users.findByToken( token );
        if( user != null)
        {
            return user;
        }

        return UNAUTHENTICATED;
    }

    /**
     * Get a user, given a name. If no user is associated with the name, return an
     * {@link org.neo4j.server.security.auth.UnauthenticatedUser unauthenticated} user.
     */
    public User userForName( String name )
    {
        if(name == null)
        {
            return UNAUTHENTICATED;
        }

        User user = users.findByName( name );
        if( user != null)
        {
            return user;
        }
        return UNAUTHENTICATED;
    }

    /** Set a new random token for a given user */
    public User regenerateToken( String name ) throws IOException
    {
        try
        {
            String token = newToken();
            return setToken( name, token );
        }
        catch ( IllegalTokenException e )
        {
            // This is technically a possibly infinite recursive loop. However, hand-wavedly, given a user regenerates
            // her token a million times per second, this branch will execute once
            // every 539 514 153 540 301 000 000 000 years. As such, it is unlikely this will loop forever.
            return regenerateToken( name );
        }
    }

    public User setToken( String name, String token ) throws IllegalTokenException, IOException
    {
        assertValidToken( token );
        User existingUser = users.findByName( name );
        if ( existingUser == null )
        {
            return null;
        }
        User updatedUser = existingUser.augment().withToken( token ).build();
        try
        {
            users.update( existingUser, updatedUser );
            return updatedUser;
        } catch ( ConcurrentModificationException e )
        {
            // try again
            return setToken( name, token );
        }
    }

    public User setPassword( String name, String password ) throws IOException
    {
        return authentication.setPassword( name, password );
    }

    private String newToken()
    {
        byte[] tokenData = new byte[16];
        rand.nextBytes( tokenData );
        return BytePrinter.compactHex(tokenData).toLowerCase();
    }

    private void assertValidName( String name )
    {
        if(!users.isValidName( name ))
        {
            throw new IllegalArgumentException( "User name contains illegal characters. Please use simple ascii characters and numbers." );
        }
    }

    private void assertValidToken( String token )
    {
        if(!users.isValidToken( token ))
        {
            throw new IllegalArgumentException( "Token is not a valid Neo4j Authorization Token." );
        }
    }
}
