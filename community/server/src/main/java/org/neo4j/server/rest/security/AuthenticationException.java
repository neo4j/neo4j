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
package org.neo4j.server.rest.security;

/**
 * <p>
 * A runtime exception representing a failure to provide correct authentication
 * credentials.
 * </p>
 */
public class AuthenticationException extends RuntimeException
{

    private static final long serialVersionUID = 3662922094534872711L;

    private final String realm;

    public AuthenticationException( String msg, String realm )
    {
        super( msg );
        this.realm = realm;
    }

    public String getRealm()
    {
        return this.realm;
    }

}
