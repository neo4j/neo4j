/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.enterprise.api.security;

import java.io.IOException;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

/**
 * A logged in user.
 */
public interface EnterpriseAuthSubject extends AuthSubject
{
    /**
     * Enterprise has the concept of users being admins.
     */
    boolean isAdmin();

    static EnterpriseAuthSubject castOrFail( AuthSubject authSubject )
    {
        return castOrFail( EnterpriseAuthSubject.class, authSubject );
    }

    static <T extends EnterpriseAuthSubject> T castOrFail( Class<T> clazz, AuthSubject authSubject )
    {
        if ( !(clazz.isInstance( authSubject )) )
        {
            throw new IllegalArgumentException( "Incorrect AuthSubject type " + authSubject.getClass().getTypeName() );
        }
        return clazz.cast( authSubject );
    }

    EnterpriseAuthSubject AUTH_DISABLED = new EnterpriseAuthSubject()
    {
        @Override
        public boolean allowsReads()
        {
            return AuthSubject.AUTH_DISABLED.allowsReads();
        }

        @Override
        public boolean allowsWrites()
        {
            return AuthSubject.AUTH_DISABLED.allowsWrites();
        }

        @Override
        public boolean allowsSchemaWrites()
        {
            return AuthSubject.AUTH_DISABLED.allowsSchemaWrites();
        }

        @Override
        public boolean overrideOriginalMode()
        {
            return AuthSubject.AUTH_DISABLED.overrideOriginalMode();
        }

        @Override
        public AuthorizationViolationException onViolation( String msg )
        {
            return AuthSubject.AUTH_DISABLED.onViolation( msg );
        }

        @Override
        public String name()
        {
            return AuthSubject.AUTH_DISABLED.name();
        }

        @Override
        public boolean isAdmin()
        {
            return true;
        }

        @Override
        public void logout()
        {
            AuthSubject.AUTH_DISABLED.logout();
        }

        @Override
        public AuthenticationResult getAuthenticationResult()
        {
            return AuthSubject.AUTH_DISABLED.getAuthenticationResult();
        }

        @Override
        public void setPassword( String password, boolean requirePasswordChange )
                throws IOException, InvalidArgumentsException
        {
            AuthSubject.AUTH_DISABLED.setPassword( password, requirePasswordChange );
        }

        @Override
        public void passwordChangeNoLongerRequired()
        {
        }

        @Override
        public boolean allowsProcedureWith( String[] roleNames ) throws InvalidArgumentsException
        {
            return AuthSubject.AUTH_DISABLED.allowsProcedureWith( roleNames );
        }

        @Override
        public String username()
        {
            return AuthSubject.AUTH_DISABLED.username();
        }

        @Override
        public boolean hasUsername( String username )
        {
            return AuthSubject.AUTH_DISABLED.hasUsername( username );
        }

        @Override
        public void ensureUserExistsWithName( String username ) throws InvalidArgumentsException
        {
            AuthSubject.AUTH_DISABLED.ensureUserExistsWithName( username );
        }
    };
}
