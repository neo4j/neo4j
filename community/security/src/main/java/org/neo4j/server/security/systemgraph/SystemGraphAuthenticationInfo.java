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
package org.neo4j.server.security.systemgraph;

import org.neo4j.server.security.auth.ShiroAuthenticationInfo;

import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.impl.security.User;

/**
 * This is used by SystemGraphRealm to cache a user record in the authentication caches
 * and update the authentication result based on the outcome of its CredentialsMatcher
 */
class SystemGraphAuthenticationInfo extends ShiroAuthenticationInfo
{
    private final User userRecord;

    SystemGraphAuthenticationInfo( User userRecord, String realmName )
    {
        super( userRecord.name(), realmName, AuthenticationResult.FAILURE );
        this.userRecord = userRecord;
    }

    User getUserRecord()
    {
        return userRecord;
    }

    void setAuthenticationResult( AuthenticationResult authenticationResult )
    {
        this.authenticationResult = authenticationResult;
    }
}
