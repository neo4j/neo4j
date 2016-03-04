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
package org.neo4j.server.security.auth;

import java.io.IOException;

import org.neo4j.kernel.api.security.AccessMode;

public class BasicAuthSubject implements AuthSubject
{
    private final BasicAuthManager authManager;
    private User user;
    private AuthenticationResult authenticationResult;
    private final AccessMode.Static accessMode;

    public BasicAuthSubject( BasicAuthManager authManager, User user, AuthenticationResult authenticationResult )
    {
        this.authManager = authManager;
        this.user = user;
        this.authenticationResult = authenticationResult;

        if ( authenticationResult == AuthenticationResult.SUCCESS )
        {
            accessMode = AccessMode.Static.FULL;
        }
        else
        {
            accessMode = AccessMode.Static.NONE;
        }
    }

    @Override
    public void logout()
    {
        user = null;
        authenticationResult = AuthenticationResult.FAILURE;
    }

    @Override
    public AuthenticationResult getAuthenticationResult()
    {
        return authenticationResult;
    }

    @Override
    public boolean setPassword( String password ) throws IOException
    {
        return authManager.setPassword( this, user.name(), password ) != null;
    }

    public boolean doesUsernameMatch( String username )
    {
        return user.name().equals( username );
    }

    @Override
    public boolean allowsReads()
    {
        return accessMode.allowsReads();
    }

    @Override
    public boolean allowsWrites()
    {
        return accessMode.allowsWrites();
    }

    @Override
    public boolean allowsSchemaWrites()
    {
        return accessMode.allowsSchemaWrites();
    }

    @Override
    public String name()
    {
        return null;
    }
}
