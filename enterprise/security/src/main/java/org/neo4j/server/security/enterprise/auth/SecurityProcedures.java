/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.security.enterprise.auth;

import java.util.stream.Stream;

import org.neo4j.graphdb.security.AuthorizationViolationException;
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
    public Stream<UserManagementProcedures.UserResult> showCurrentUserDeprecated()
    {
        return showCurrentUser();
    }

    @Description( "Show the current user." )
    @Procedure( name = "dbms.showCurrentUser", mode = DBMS )
    public Stream<UserManagementProcedures.UserResult> showCurrentUser()
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
