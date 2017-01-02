/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth.plugin.spi;

import java.io.Serializable;
import java.util.Collection;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;

/**
 * An object that can be returned as the result of successful authentication by an <tt>AuthPlugin</tt>.
 *
 * <p>This result type combines authentication and authorization information.
 *
 * <p>NOTE: If authentication caching is enabled the result type <tt>CacheableAuthInfo</tt> should be used instead.
 *
 * @see AuthPlugin#authenticateAndAuthorize(AuthToken)
 * @see CacheableAuthInfo
 */
public interface AuthInfo extends Serializable
{
    /**
     * Should return a principal that uniquely identifies the authenticated subject within this auth provider.
     *
     * <p>Typically this is the same as the principal within the auth token map.
     *
     * @return a principal that uniquely identifies the authenticated subject within this auth provider.
     */
    Object principal();

    /**
     * Should return the roles assigned to this principal.
     *
     * @return the roles assigned to this principal
     */
    Collection<String> roles();

    static AuthInfo of( Object principal, Collection<String> roles )
    {
        return new AuthInfo()
        {
            @Override
            public Object principal()
            {
                return principal;
            }

            @Override
            public Collection<String> roles()
            {
                return roles;
            }
        };
    }
}
