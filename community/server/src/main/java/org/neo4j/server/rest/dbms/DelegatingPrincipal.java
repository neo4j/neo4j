/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.server.rest.dbms;

import java.security.Principal;

import org.neo4j.internal.kernel.api.security.SecurityContext;

public class DelegatingPrincipal implements Principal
{
    private String username;
    private final SecurityContext securityContext;

    DelegatingPrincipal( String username, SecurityContext securityContext )
    {
        this.username = username;
        this.securityContext = securityContext;
    }

    @Override
    public String getName()
    {
        return username;
    }

    public SecurityContext getSecurityContext()
    {
        return securityContext;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !( o instanceof DelegatingPrincipal) )
        {
            return false;
        }

        DelegatingPrincipal that = (DelegatingPrincipal) o;
        return username.equals( that.username );
    }

    @Override
    public int hashCode()
    {
        return username.hashCode();
    }

    @Override
    public String toString()
    {
        return "DelegatingPrincipal{" +
                "username='" + username + '\'' +
                '}';
    }
}
