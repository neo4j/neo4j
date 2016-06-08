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
package org.neo4j.server.security.enterprise.auth;

import java.io.IOException;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.exception.IllegalCredentialsException;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsDBMS;
import org.neo4j.procedure.Procedure;
import org.neo4j.server.security.auth.exception.ConcurrentModificationException;

public class AuthProcedures
{
    public static final String PERMISSION_DENIED = "Permission denied";

    @Context
    public AuthSubject authSubject;

    @PerformsDBMS
    @Procedure( "dbms.createUser" )
    public void createUser( @Name( "username" ) String username, @Name( "password" ) String password,
            @Name( "requirePasswordChange" ) boolean requirePasswordChange )
            throws IllegalCredentialsException, IOException
    {
        ShiroAuthSubject shiroSubject = ShiroAuthSubject.castOrFail( authSubject );
        if ( !shiroSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        shiroSubject.getUserManager().newUser( username, password, requirePasswordChange );
    }

    @PerformsDBMS
    @Procedure( "dbms.addUserToRole" )
    public void addUserToRole( @Name( "username" ) String username, @Name( "roleName" ) String roleName ) throws IOException
    {
        ShiroAuthSubject shiroSubject = ShiroAuthSubject.castOrFail( authSubject );
        if ( !shiroSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        shiroSubject.getRoleManager().addUserToRole( username, roleName );
    }

    @PerformsDBMS
    @Procedure( "dbms.removeUserFromRole" )
    public void removeUserFromRole( @Name( "username" ) String username, @Name( "roleName" ) String roleName )
            throws IllegalCredentialsException, IOException
    {
        ShiroAuthSubject shiroSubject = ShiroAuthSubject.castOrFail( authSubject );
        if ( !shiroSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        shiroSubject.getRoleManager().removeUserFromRole( username, roleName );
    }

    @PerformsDBMS
    @Procedure( "dbms.deleteUser" )
    public void deleteUser( @Name( "username" ) String username ) throws IllegalCredentialsException, IOException
    {
        ShiroAuthSubject shiroSubject = ShiroAuthSubject.castOrFail( authSubject );
        if ( !shiroSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        shiroSubject.getUserManager().deleteUser( username );
    }

    @PerformsDBMS
    @Procedure( "dbms.suspendUser" )
    public void suspendUser( @Name( "username" ) String username ) throws IOException
    {
        ShiroAuthSubject shiroSubject = ShiroAuthSubject.castOrFail( authSubject );
        if ( !shiroSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        shiroSubject.getUserManager().suspendUser( username );
    }

    @PerformsDBMS
    @Procedure( "dbms.activateUser" )
    public void activateUser( @Name( "username" ) String username ) throws IOException
    {
        ShiroAuthSubject shiroSubject = ShiroAuthSubject.castOrFail( authSubject );
        if ( !shiroSubject.isAdmin() )
        {
            throw new AuthorizationViolationException( PERMISSION_DENIED );
        }
        shiroSubject.getUserManager().activateUser( username );
    }
}
