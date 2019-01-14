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
package org.neo4j.internal.kernel.api.security;

public interface AuthSubject
{
    void logout();

    // TODO: Refine this API into something more polished
    AuthenticationResult getAuthenticationResult();

    /**
     * Changes the {@link AuthenticationResult} status to {@link AuthenticationResult#SUCCESS SUCCESS}
     * if it was {@link AuthenticationResult#PASSWORD_CHANGE_REQUIRED PASSWORD_CHANGE_REQUIRED}.
     * This allows users that changed their password to become authorized for continued processing.
     */
    void setPasswordChangeNoLongerRequired();

    /**
     * @param username a username
     * @return true if the provided username is the underlying user name of this subject
     */
    boolean hasUsername( String username );

    /**
     * Get the username associated with the auth subject
     * @return the username
     */
    String username();

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
        public void setPasswordChangeNoLongerRequired()
        {
        }

        @Override
        public boolean hasUsername( String username )
        {
            return false;
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
        public void setPasswordChangeNoLongerRequired()
        {
        }

        @Override
        public boolean hasUsername( String username )
        {
            return false;
        }
    };
}
