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

import java.util.function.Function;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;

/**
 * Controls the capabilities of a KernelTransaction, including the authenticated user and authorization data.
 *
 * Must extend LoginContext to handle procedures creating internal transactions, periodic commit and the parallel cypher prototype.
 */
public class SecurityContext implements LoginContext
{
    protected final AuthSubject subject;
    protected final AccessMode mode;

    public SecurityContext( AuthSubject subject, AccessMode mode )
    {
        this.subject = subject;
        this.mode = mode;
    }

    /**
     * Get the authorization data of the user. This is immutable.
     */
    public AccessMode mode()
    {
        return mode;
    }

    /**
     * Check whether the user is an admin.
     */
    public boolean isAdmin()
    {
        return true;
    }

    @Override
    public AuthSubject subject()
    {
        return subject;
    }

    @Override
    public SecurityContext authorize( Function<String, Integer> propertyIdLookup )
    {
        return this;
    }

    /**
     * Create a copy of this SecurityContext with the provided mode.
     */
    public SecurityContext withMode( AccessMode mode )
    {
        return new SecurityContext( subject, mode );
    }

    public void assertCredentialsNotExpired()
    {
        if ( subject().getAuthenticationResult().equals( AuthenticationResult.PASSWORD_CHANGE_REQUIRED ) )
        {
            throw mode().onViolation( PERMISSION_DENIED );
        }
    }

    public String description()
    {
        return String.format( "user '%s' with %s", subject().username(), mode().name() );
    }

    protected String defaultString( String name )
    {
        return String.format( "%s{ username=%s, accessMode=%s }", name, subject().username(), mode() );
    }

    /** Allows all operations. */
    @SuppressWarnings( "StaticInitializerReferencesSubClass" )
    public static final SecurityContext AUTH_DISABLED = new AuthDisabled( AccessMode.Static.FULL );

    private static class AuthDisabled extends SecurityContext
    {
        private AuthDisabled( AccessMode mode )
        {
            super( AuthSubject.AUTH_DISABLED, mode );
        }

        @Override
        public SecurityContext withMode( AccessMode mode )
        {
            return new AuthDisabled( mode );
        }

        @Override
        public String description()
        {
            return "AUTH_DISABLED with " + mode().name();
        }

        @Override
        public String toString()
        {
            return defaultString( "auth-disabled" );
        }
    }
}
