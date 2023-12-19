/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
 * <p>This is an alternative to {@link CacheableAuthenticationInfo} to use if you want to manage your own way of
 * hashing and matching credentials. On authentication, when a cached authentication info from a previous successful
 * authentication attempt is found for the principal within the auth token map, then
 * {@link CredentialsMatcher#doCredentialsMatch(AuthToken)} of the {@link CredentialsMatcher} returned by
 * {@link #credentialsMatcher()} will be called to determine if the credentials match.
 *
 * <p>NOTE: Caching only occurs if it is explicitly enabled by the plugin.
 *
 * @see AuthenticationPlugin#authenticate(AuthToken)
 * @see AuthProviderOperations#setAuthenticationCachingEnabled(boolean)
 */
public interface CustomCacheableAuthenticationInfo extends AuthenticationInfo
{
    interface CredentialsMatcher
    {
        /**
         * Returns true if the credentials of the given {@link AuthToken} matches the credentials of the cached
         * {@link CustomCacheableAuthenticationInfo} that is the owner of this {@link CredentialsMatcher}.
         *
         * @return true if the credentials of the given auth token matches the credentials of this cached
         *         authentication info, otherwise false
         */
        boolean doCredentialsMatch( AuthToken authToken );
    }

    /**
     * Returns the credentials matcher that will be used to verify the credentials of an auth token against the
     * cached credentials in this object.
     *
     * <p>NOTE: The returned object implementing the {@link CredentialsMatcher} interface need to have a
     * reference to the actual credentials in a matcheable form within its context in order to benefit from caching,
     * so it is typically stateful. The simplest way is to return a lambda from this method.
     *
     * @return the credentials matcher that will be used to verify the credentials of an auth token against the
     *         cached credentials in this object
     */
    CredentialsMatcher credentialsMatcher();

    static CustomCacheableAuthenticationInfo of( Object principal, CredentialsMatcher credentialsMatcher )
    {
        return new CustomCacheableAuthenticationInfo()
        {
            @Override
            public Object principal()
            {
                return principal;
            }

            @Override
            public CredentialsMatcher credentialsMatcher()
            {
                return credentialsMatcher;
            }
        };
    }
}
