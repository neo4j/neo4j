/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.internal.kernel.api.security;

import org.neo4j.internal.kernel.api.Token;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;

/**
 * Controls the capabilities of a KernelTransaction, including the authenticated user and authorization data.
 *
 * Must extend LoginContext to handle procedures creating internal transactions, periodic commit and the parallel cypher prototype.
 */
public interface SecurityContext extends LoginContext
{
    /**
     * Get the authorization data of the user. This is immutable.
     */
    AccessMode mode();

    /**
     * Check whether the user is an admin.
     */
    boolean isAdmin();

    /**
     * Create a copy of this SecurityContext with the provided mode.
     */
    SecurityContext withMode( AccessMode mode );

    default void assertCredentialsNotExpired()
    {
        if ( subject().getAuthenticationResult().equals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) )
        {
            throw mode().onViolation( PERMISSION_DENIED );
        }
    }

    default String description()
    {
        return String.format( "user '%s' with %s", subject().username(), mode().name() );
    }

    default String defaultString( String name )
    {
        return String.format( "%s{ username=%s, accessMode=%s }", name, subject().username(), mode() );
    }

    /** Allows all operations. */
    SecurityContext AUTH_DISABLED = new AuthDisabled( AccessMode.Static.FULL );

    final class AuthDisabled implements SecurityContext
    {
        private final AccessMode mode;

        private AuthDisabled( AccessMode mode )
        {
            this.mode = mode;
        }

        @Override
        public AccessMode mode()
        {
            return mode;
        }

        @Override
        public AuthSubject subject()
        {
            return AuthSubject.AUTH_DISABLED;
        }

        @Override
        public boolean isAdmin()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return defaultString( "auth-disabled" );
        }

        @Override
        public String description()
        {
            return "AUTH_DISABLED with " + mode.name();
        }

        @Override
        public SecurityContext freeze( Token token )
        {
            return this;
        }

        @Override
        public SecurityContext withMode( AccessMode mode )
        {
            return new AuthDisabled( mode );
        }
    }

    final class Frozen implements SecurityContext
    {
        private final AuthSubject subject;
        private final AccessMode mode;

        public Frozen( AuthSubject subject, AccessMode mode )
        {
            this.subject = subject;
            this.mode = mode;
        }

        @Override
        public AccessMode mode()
        {
            return mode;
        }

        @Override
        public AuthSubject subject()
        {
            return subject;
        }

        @Override
        public boolean isAdmin()
        {
            return true;
        }

        @Override
        public SecurityContext freeze( Token token )
        {
            return this;
        }

        @Override
        public SecurityContext withMode( AccessMode mode )
        {
            return new Frozen( subject, mode );
        }
    }
}
