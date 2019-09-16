/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.rest.repr;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.neo4j.kernel.impl.security.User;

public class AuthorizationRepresentation
{
    @JsonProperty( "username" )
    private final String userName;
    @JsonProperty( "password_change_required" )
    private final boolean passwordChangeRequired;
    @JsonProperty( "password_change" )
    private final String passwordChange;

    public AuthorizationRepresentation( User user, String passwordChange )
    {
        this.userName = user.name();
        this.passwordChangeRequired = user.passwordChangeRequired();
        this.passwordChange = passwordChange;
    }

    public String getUserName()
    {
        return userName;
    }

    public boolean isPasswordChangeRequired()
    {
        return passwordChangeRequired;
    }

    public String getPasswordChange()
    {
        return passwordChange;
    }
}
