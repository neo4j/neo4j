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
package org.neo4j.server.rest.dbms;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.security.Principal;

public class AuthorizedRequestWrapper extends HttpServletRequestWrapper
{
    private class DelegatingPrinciple implements Principal
    {
        private String username;

        private DelegatingPrinciple( String username )
        {
            this.username = username;
        }

        @Override
        public String getName()
        {
            return username;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( !( o instanceof DelegatingPrinciple ) )
            {
                return false;
            }

            DelegatingPrinciple that = (DelegatingPrinciple) o;
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
            return "DelegatingPrinciple{" +
                    "username='" + username + '\'' +
                    '}';
        }
    }

    private final String authType;
    private final DelegatingPrinciple principle;

    public AuthorizedRequestWrapper( final String authType, final String username, final HttpServletRequest request )
    {
        super( request );
        this.authType = authType;
        this.principle = new DelegatingPrinciple( username );
    }

    @Override
    public String getAuthType()
    {
        return authType;
    }

    @Override
    public Principal getUserPrincipal()
    {
        return principle;
    }

    @Override
    public boolean isUserInRole( String role )
    {
        return true;
    }

    @Override
    public String toString()
    {
        return "AuthorizedRequestWrapper{" +
                "authType='" + authType + '\'' +
                ", principle=" + principle +
                '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        AuthorizedRequestWrapper that = (AuthorizedRequestWrapper) o;
        if ( !authType.equals( that.authType ) )
        {
            return false;
        }
        if ( !principle.equals( that.principle ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = authType.hashCode();
        result = 31 * result + principle.hashCode();
        return result;
    }
}
