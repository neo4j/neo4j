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

import org.neo4j.server.security.auth.exception.IllegalCredentialsException;

/**
 * An AuthManager is used to do basic authentication and user management.
 */
public interface AuthManager
{

    /**
     * Authenticate a username and password
     * @param username The name of the user
     * @param password The password of the user
     */
    AuthenticationResult authenticate( String username, String password );

    /**
     * Log in using the provided username and password
     * @param username The name of the user
     * @param password The password of the user
     * @return An AuthSubject representing the newly logged-in user
     */
    AuthSubject login( String username, String password );

    /**
     * Create a new user with the provided credentials.
     * @param username The name of the user.
     * @param initialPassword The initial password.
     * @param requirePasswordChange Does the user need to change the initial password.
     * @return A new user with the provided credentials.
     * @throws IOException If user can't be serialized to disk.
     * @throws IllegalCredentialsException If the username is invalid.
     */
    User newUser( String username, String initialPassword, boolean requirePasswordChange ) throws IOException,
            IllegalCredentialsException;

    /**
     * Delete the given user
     * @param username the name of the user to delete.
     * @return <tt>true</tt> is user was deleted otherwise <tt>false</tt>
     * @throws IOException
     */
    boolean deleteUser( String username ) throws IOException;

    /**
     * Retrieves the user with the provided user name.
     * @param username The name of the user to retrieve.
     * @return The stored user with the given user name.
     */
    User getUser( String username );

    /**
     * Set the password of the provided user.
     * @param username The name of the user whose password should be set.
     * @param password The new password for the user.
     * @return User with updated credentials
     * @throws IOException
     */
    User setPassword( String username, String password ) throws IOException;

    /**
     * Implementation that does no authentication.
     */
    AuthManager NO_AUTH = new AuthManager()
    {
        @Override
        public AuthenticationResult authenticate( String username, String password )
        {
            return AuthenticationResult.SUCCESS;
        }

        @Override
        public AuthSubject login( String username, String password )
        {
            return new AuthSubject()
            {
                @Override
                public void logout()
                {
                }

                @Override
                public AuthenticationResult getAuthenticationResult()
                {
                    return AuthenticationResult.SUCCESS;
                }

                @Override
                public void setPassword( String password ) throws IOException, IllegalCredentialsException
                {
                }

                @Override
                public boolean allowsReads()
                {
                    return true;
                }

                @Override
                public boolean allowsWrites()
                {
                    return true;
                }

                @Override
                public boolean allowsSchemaWrites()
                {
                    return true;
                }

                @Override
                public String name()
                {
                    return "";
                }
            };
        }

        @Override
        public User newUser( String username, String initialPassword, boolean requirePasswordChange )
                throws IOException, IllegalCredentialsException
        {
            return new User.Builder(  )
                    .withName( username )
                    .withCredentials( Credential.forPassword( initialPassword ) )
                    .withRequiredPasswordChange( requirePasswordChange )
                    .build();
        }

        @Override
        public boolean deleteUser( String username ) throws IOException
        {
            return true;
        }

        @Override
        public User getUser( String username )
        {
            return new User.Builder().withName( username ).build();
        }

        @Override
        public User setPassword( String username, String password ) throws IOException
        {
            return new User.Builder(  )
                    .withName( username )
                    .withCredentials( Credential.forPassword( password ) )
                    .build();
        }
    };
}
