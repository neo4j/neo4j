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

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthProviderOperations;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;

/**
 * A cacheable object that can be returned as the result of successful authentication by an
 * {@link AuthenticationPlugin}.
 *
 * <p>This object can be cached by the Neo4j authentication cache.
 *
 * <p>This is an alternative to {@link CustomCacheableAuthenticationInfo} if you want Neo4j to manage secure
 * hashing and matching of cached credentials.
 *
 * <p>NOTE: Caching only occurs if it is explicitly enabled by the plugin.
 *
 * @see AuthenticationPlugin#authenticate(AuthToken)
 * @see AuthProviderOperations#setAuthenticationCachingEnabled(boolean)
 * @see CustomCacheableAuthenticationInfo
 */
public interface CacheableAuthenticationInfo extends AuthenticationInfo
{
    /**
     * Should return a principal that uniquely identifies the authenticated subject within this authentication provider.
     * This will be used as the cache key, and needs to be matcheable against a principal within the auth token map.
     *
     * <p>Typically this is the same as the principal within the auth token map.
     *
     * @return a principal that uniquely identifies the authenticated subject within this authentication provider
     *
     * @see AuthToken#principal()
     */
    @Override
    Object principal();

    /**
     * Should return credentials that can be cached, so that successive authentication attempts could be performed
     * against the cached authentication info from a previous successful authentication attempt.
     *
     * <p>NOTE: The returned credentials will be hashed using a cryptographic hash function together
     * with a random salt (generated with a secure random number generator) before being stored.
     *
     * @return credentials that can be cached
     *
     * @see AuthToken#credentials()
     * @see AuthenticationPlugin#authenticate(AuthToken)
     */
    byte[] credentials();

    /**
     * Creates a new {@link CacheableAuthenticationInfo}
     *
     * @param principal a principal that uniquely identifies the authenticated subject within this authentication
     *                  provider
     * @param credentials credentials that can be cached
     * @return a new {@link CacheableAuthenticationInfo} containing the given parameters
     */
    static CacheableAuthenticationInfo of( Object principal, byte[] credentials )
    {
        return new CacheableAuthenticationInfo()
        {
            @Override
            public Object principal()
            {
                return principal;
            }

            @Override
            public byte[] credentials()
            {
                return credentials;
            }
        };
    }
}
