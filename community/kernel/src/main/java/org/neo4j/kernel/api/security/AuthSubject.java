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
package org.neo4j.kernel.api.security;

import java.io.IOException;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

public interface AuthSubject extends AccessMode
{
    void logout();

    // TODO: Refine this API into something more polished
    AuthenticationResult getAuthenticationResult();

    /**
     * Set the password for the AuthSubject
     * @param password The new password
     * @param requirePasswordChange
     * @throws IOException If the new credentials cannot be serialized to disk.
     * @throws InvalidArgumentsException If the new password is invalid.
     */
    void setPassword( String password, boolean requirePasswordChange ) throws IOException, InvalidArgumentsException;

    /**
     * Changes authentication status to SUCCESS if in PASSWORD_CHANGE_REQUIRED
     */
    void passwordChangeNoLongerRequired();

    /**
     * Determines whether this subject is allowed to execute a procedure with the parameter string in its procedure annotation.
     * @param roleNames
     * @return
     * @throws InvalidArgumentsException
     */
    boolean allowsProcedureWith( String[] roleNames ) throws InvalidArgumentsException;

    /**
     * @return A string representing the primary principal of this subject
     */
    String username();

    /**
     * @param username a username
     * @return true if the provided username is the underlying user name of this subject
     */
    boolean hasUsername( String username );

    /**
     * Ensure that the provided username is the name of an existing user known to the system.
     *
     * @param username a username
     * @throws InvalidArgumentsException if the provided user name is not the name of an existing user
     */
    default void ensureUserExistsWithName( String username ) throws InvalidArgumentsException {
        throw new InvalidArgumentsException( "User '" + username + "' does not exit." );
    }

    /**
     * Implementation to use when authentication has not yet been performed. Allows nothing.
     */
    AuthSubject ANONYMOUS = new AuthSubject()
    {
        @Override
        public void logout()
        {
        }

        @Override
        public AuthenticationResult getAuthenticationResult()
        {
            return AuthenticationResult.FAILURE;
        }

        @Override
        public void setPassword( String password, boolean requirePasswordChange )
                throws IOException, InvalidArgumentsException
        {
            throw new AuthorizationViolationException( "Anonymous cannot change password" );
        }

        @Override
        public void passwordChangeNoLongerRequired()
        {
        }

        @Override
        public boolean hasUsername( String username )
        {
            return false;
        }

        @Override
        public boolean allowsProcedureWith( String[] roleNames )
        {
            return false;
        }

        @Override
        public boolean allowsReads()
        {
            return false;
        }

        @Override
        public boolean allowsWrites()
        {
            return false;
        }

        @Override
        public boolean allowsSchemaWrites()
        {
            return false;
        }

        @Override
        public boolean overrideOriginalMode()
        {
            return false;
        }

        @Override
        public AuthorizationViolationException onViolation( String msg )
        {
            return new AuthorizationViolationException( msg );
        }

        @Override
        public String name()
        {
            return "<anonymous>";
        }

        @Override
        public String username()
        {
            return ""; // Should never clash with a valid username
        }
    };

    /**
     * Implementation to use when authentication is disabled. Allows everything.
     */
    AuthSubject AUTH_DISABLED = new AuthSubject()
    {
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
        public boolean overrideOriginalMode()
        {
            return false;
        }

        @Override
        public AuthorizationViolationException onViolation( String msg )
        {
            return new AuthorizationViolationException( msg );
        }

        @Override
        public String name()
        {
            return "<auth disabled>";
        }

        @Override
        public String username()
        {
            return ""; // Should never clash with a valid username
        }

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
        public void setPassword( String password, boolean requirePasswordChange )
                throws IOException, InvalidArgumentsException
        {
        }

        @Override
        public void passwordChangeNoLongerRequired()
        {
        }

        @Override
        public boolean hasUsername( String username )
        {
            return false;
        }

        @Override
        public boolean allowsProcedureWith( String[] roleNames )
        {
            return true;
        }
    };
}
