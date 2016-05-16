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

import java.util.Collections;
import java.util.Set;

import org.neo4j.server.security.auth.BasicAuthManager;
import org.neo4j.server.security.auth.User;

public class PredefinedGroupsAccountBuilder implements AccountBuilder
{
    @Override
    public UserAccount buildAccount( User user )
    {
        Set<String> roleNames = getRolesForGroup( user.group() );
        return new UserAccount( roleNames );
    }

    private Set<String> getRolesForGroup( String group )
    {
        switch ( group )
        {
        case "admin":
        case "architect":
        case BasicAuthManager.DEFAULT_GROUP:
            return Collections.singleton( ShiroAuthSubject.SCHEMA_READ_WRITE );
        case "publisher":
            return Collections.singleton( ShiroAuthSubject.READ_WRITE );
        case "reader":
            return Collections.singleton( ShiroAuthSubject.READ );
        default:
            return Collections.emptySet();
        }
    }
}
