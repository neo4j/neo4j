/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.security.User;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.FAILURE;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.TOO_MANY_ATTEMPTS;

public class BasicLoginContext extends LoginContext
{
    private AccessMode accessMode;

    public BasicLoginContext( User user, AuthenticationResult authenticationResult, ClientConnectionInfo connectionInfo )
    {
        super( new BasicAuthSubject( user, authenticationResult ), connectionInfo );

        switch ( authenticationResult )
        {
        case SUCCESS:
            accessMode = AccessMode.Static.FULL;
            break;
        case PASSWORD_CHANGE_REQUIRED:
            accessMode = AccessMode.Static.CREDENTIALS_EXPIRED;
            break;
        default:
            accessMode = AccessMode.Static.ACCESS;
        }
    }

    private static class BasicAuthSubject implements AuthSubject
    {
        private User user;
        private AuthenticationResult authenticationResult;

        BasicAuthSubject( User user, AuthenticationResult authenticationResult )
        {
            this.user = user;
            this.authenticationResult = authenticationResult;
        }

        @Override
        public AuthenticationResult getAuthenticationResult()
        {
            return authenticationResult;
        }

        @Override
        public String username()
        {
            return user.name();
        }

        @Override
        public boolean hasUsername( String username )
        {
            return username().equals( username );
        }
    }

    @Override
    public SecurityContext authorize( IdLookup idLookup, String dbName )
    {
        if ( subject().getAuthenticationResult().equals( FAILURE ) || subject().getAuthenticationResult().equals( TOO_MANY_ATTEMPTS ) )
        {
            throw new AuthorizationViolationException( AuthorizationViolationException.PERMISSION_DENIED, Status.Security.Unauthorized );
        }
        else if ( !dbName.equals( SYSTEM_DATABASE_NAME ) && subject().getAuthenticationResult().equals( PASSWORD_CHANGE_REQUIRED ) )
        {
            throw AccessMode.Static.CREDENTIALS_EXPIRED.onViolation( AuthorizationViolationException.PERMISSION_DENIED );
        }
        return new SecurityContext( subject(), accessMode, connectionInfo() );
    }
}
