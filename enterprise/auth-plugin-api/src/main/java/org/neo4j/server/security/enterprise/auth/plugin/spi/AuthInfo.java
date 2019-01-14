/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.security.enterprise.auth.plugin.spi;

import java.io.Serializable;
import java.util.Collection;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;

/**
 * An object that can be returned as the result of successful authentication by an {@link AuthPlugin}.
 *
 * <p>This result type combines authentication and authorization information.
 *
 * <p>NOTE: If authentication caching is enabled the result type {@link CacheableAuthInfo} should be used instead.
 *
 * @see AuthPlugin#authenticateAndAuthorize(AuthToken)
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
