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
import org.neo4j.kernel.api.security.exception.InvalidArgumentsException;

public interface AuthSubject extends AccessMode
{
    void logout();

    // TODO: Refine this API into something more polished
    AuthenticationResult getAuthenticationResult();

    /**
     * Set the password for the AuthSubject
     * @param password The new password
     * @throws IOException If the new credentials cannot be serialized to disk.
     * @throws InvalidArgumentsException If the new password is invalid.
     */
    void setPassword( String password ) throws IOException, InvalidArgumentsException;

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
        public void setPassword( String password ) throws IOException, InvalidArgumentsException
        {
            throw new AuthorizationViolationException( "Anonymous cannot change password" );
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
        public void logout()
        {
        }

        @Override
        public AuthenticationResult getAuthenticationResult()
        {
            return AuthenticationResult.SUCCESS;
        }

        @Override
        public void setPassword( String password ) throws IOException, InvalidArgumentsException
        {
        }
    };
}
