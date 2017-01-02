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

import java.util.Collection;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthProviderOperations;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;

/**
 * A cacheable object that can be returned as the result of successful authentication by an
 * <tt>AuthPlugin</tt>.
 *
 * <p>This object can be cached by the Neo4j authentication cache.
 *
 * <p>This result type is used if you want Neo4j to manage secure hashing and matching of cached credentials.
 * If you instead want to manage this yourself you need to use the separate interfaces
 * <tt>AuthenticationPlugin</tt> and <tt>AuthorizationPlugin</tt> together with
 * a <tt>CustomCacheableAuthenticationInfo</tt> result.
 *
 * <p>NOTE: Caching of authentication info only occurs if it is explicitly enabled by the plugin, whereas
 * caching of authorization info (assigned roles) is enabled by default.
 *
 * <p>NOTE: Caching of the authorization info (assigned roles) does not require the use of a <tt>CacheableAuthInfo</tt>
 * but will work fine with a regular <tt>AuthInfo</tt>.
 *
 * @see AuthPlugin#authenticateAndAuthorize(AuthToken)
 * @see AuthProviderOperations#setAuthenticationCachingEnabled(boolean)
 * @see AuthInfo
 * @see AuthenticationPlugin
 * @see AuthorizationPlugin
 * @see CustomCacheableAuthenticationInfo
 */
public interface CacheableAuthInfo extends AuthInfo
{
    /**
     * Should return a principal that uniquely identifies the authenticated subject within this auth provider.
     * This will be used as the cache key, and needs to be matcheable against a principal within the auth token map.
     *
     * <p>Typically this is the same as the principal within the auth token map.
     *
     * @return a principal that uniquely identifies the authenticated subject within this auth provider
     *
     * @see AuthToken#principal()
     */
    @Override
    Object principal();

    /**
     * Should return credentials that can be cached, so that successive authentication attempts could be performed
     * against the cached authentication info from a previous successful authentication attempt, without having to
     * call <tt>AuthInfo.getAuthInfo()</tt> again.
     *
     * <p>NOTE: The returned credentials will be hashed using a cryptographic hash function together
     * with a random salt (generated with a secure random number generator) before being stored.
     *
     * @return credentials that can be cached
     *
     * @see AuthToken#credentials()
     * @see AuthPlugin#authenticateAndAuthorize(AuthToken)
     */
    byte[] credentials();

    static CacheableAuthInfo of( Object principal, byte[] credentials, Collection<String> roles )
    {
        return new CacheableAuthInfo()
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

            @Override
            public Collection<String> roles()
            {
                return roles;
            }
        };
    }
}
