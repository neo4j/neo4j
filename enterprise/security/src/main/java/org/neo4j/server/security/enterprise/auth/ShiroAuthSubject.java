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

import org.apache.shiro.subject.Subject;

import java.io.IOException;

import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.api.security.exception.IllegalCredentialsException;
import org.neo4j.server.security.auth.UserManager;

public class ShiroAuthSubject implements AuthSubject
{
    static final String SCHEMA_READ_WRITE = "schema:read,write";
    static final String READ_WRITE = "data:read,write";
    static final String READ = "data:read";

    private final ShiroAuthManager authManager;
    private final Subject subject;
    private final AuthenticationResult authenticationResult;

    public static ShiroAuthSubject castOrFail( AuthSubject authSubject )
    {
        if ( !(authSubject instanceof ShiroAuthSubject) )
        {
            throw new IllegalArgumentException( "Incorrect AuthSubject type " + authSubject.getClass().getTypeName() );
        }
        return (ShiroAuthSubject) authSubject;
    }

    public ShiroAuthSubject( ShiroAuthManager authManager, Subject subject, AuthenticationResult authenticationResult )
    {
        this.authManager = authManager;
        this.subject = subject;
        this.authenticationResult = authenticationResult;
    }

    @Override
    public void logout()
    {
        subject.logout();
    }

    @Override
    public AuthenticationResult getAuthenticationResult()
    {
        return authenticationResult;
    }

    @Override
    public void setPassword( String password ) throws IOException, IllegalCredentialsException
    {
        authManager.setPassword( this, (String) subject.getPrincipal(), password );
    }

    public RoleManager getRoleManager()
    {
        return authManager;
    }

    public UserManager getUserManager()
    {
        return authManager;
    }

    public boolean isAdmin()
    {
        return subject.isPermitted( "*" );
    }

    public boolean doesUsernameMatch( String username )
    {
        Object principal = subject.getPrincipal();
        return principal != null && username.equals( principal );
    }

    @Override
    public boolean allowsReads()
    {
        return getAccesMode().allowsReads();
    }

    @Override
    public boolean allowsWrites()
    {
        return getAccesMode().allowsWrites();
    }

    @Override
    public boolean allowsSchemaWrites()
    {
        return getAccesMode().allowsSchemaWrites();
    }

    @Override
    public String name()
    {
        return subject.getPrincipal().toString();
    }

    Subject getSubject()
    {
        return subject;
    }

    private AccessMode.Static getAccesMode()
    {
        if ( subject.isAuthenticated() )
        {
            if ( subject.isPermitted( SCHEMA_READ_WRITE ) )
            {
                return AccessMode.Static.FULL;
            }
            else if ( subject.isPermitted( READ_WRITE ) )
            {
                return AccessMode.Static.WRITE;
            }
            else if ( subject.isPermitted( READ ) )
            {
                return AccessMode.Static.READ;
            }
        }
        return AccessMode.Static.NONE;
    }
}
