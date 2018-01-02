/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth;

import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.procedure.Mode.DBMS;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class SecurityProcedures extends AuthProceduresBase
{
    @Context
    public EnterpriseAuthManager authManager;

    @Deprecated
    @Description( "Show the current user. Deprecated by dbms.showCurrentUser." )
    @Procedure( name = "dbms.security.showCurrentUser", mode = DBMS, deprecatedBy = "dbms.showCurrentUser" )
    public Stream<UserManagementProcedures.UserResult> showCurrentUserDeprecated() throws InvalidArgumentsException, IOException
    {
        return showCurrentUser();
    }

    @Description( "Show the current user." )
    @Procedure( name = "dbms.showCurrentUser", mode = DBMS )
    public Stream<UserManagementProcedures.UserResult> showCurrentUser() throws InvalidArgumentsException, IOException
    {
        return Stream.of( userResultForSubject() );
    }

    @Description( "Clears authentication and authorization cache." )
    @Procedure( name = "dbms.security.clearAuthCache", mode = DBMS )
    public void clearAuthenticationCache()
    {
        securityContext.assertCredentialsNotExpired();
        if ( !securityContext.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        authManager.clearAuthCache();
    }
}
