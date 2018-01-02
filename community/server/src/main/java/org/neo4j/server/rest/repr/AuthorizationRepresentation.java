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
package org.neo4j.server.rest.repr;

import org.neo4j.server.security.auth.User;

import static java.lang.String.format;

public class AuthorizationRepresentation extends ObjectRepresentation
{
    private final User user;

    public AuthorizationRepresentation( User user )
    {
        super( RepresentationType.AUTHORIZATION );
        this.user = user;
    }

    @Mapping("username")
    public ValueRepresentation user() { return ValueRepresentation.string( user.name() ); }

    @Mapping("password_change_required")
    public ValueRepresentation passwordChangeRequired() { return ValueRepresentation.bool( user.passwordChangeRequired() ); }

    @Mapping( "password_change" )
    public ValueRepresentation passwordChange()
    {
        return ValueRepresentation.uri( format( "/user/%s/password", user.name() ) );
    }
}
