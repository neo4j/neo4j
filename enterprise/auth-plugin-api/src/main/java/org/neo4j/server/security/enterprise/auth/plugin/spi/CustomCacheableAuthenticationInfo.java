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

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthProviderOperations;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;

/**
 * A cacheable object that can be returned as the result of successful authentication by an
 * <tt>AuthenticationPlugin</tt>.
 *
 * <p>This object can be cached by the Neo4j authentication cache.
 *
 * <p>This is an alternative to <tt>CacheableAuthenticationInfo</tt> to use if you want to manage your own way of
 * hashing and matching credentials. On authentication, when a cached authentication info from a previous successful
 * authentication attempt is found for the principal within the auth token map, then <tt>doCredentialsMatch</tt>
 * of the <tt>CredentialsMatcher</tt> returned by <tt>credentialsMatcher</tt> will be called to determine
 * if the credentials match.
 *
 * <p>NOTE: Caching only occurs if it is explicitly enabled by the plugin.
 *
 * @see AuthenticationPlugin#authenticate(AuthToken)
 * @see AuthProviderOperations#setAuthenticationCachingEnabled(boolean)
 * @see CacheableAuthenticationInfo
 */
public interface CustomCacheableAuthenticationInfo extends AuthenticationInfo
{
    interface CredentialsMatcher
    {
        /**
         * Returns true if the credentials of the given <tt>AuthToken</tt> matches the credentials of the cached
         * <tt>CustomCacheableAuthenticationInfo</tt> that is the owner of this <tt>CredentialsMatcher</tt>.
         *
         * @param authToken
         * @return true if the credentials of the given auth token matches the credentials of this cached
         *         authentication info, otherwise false
         */
        boolean doCredentialsMatch( AuthToken authToken );
    }

    /**
     * Returns the credentials matcher that will be used to verify the credentials of an auth token against the
     * cached credentials in this object.
     *
     * <p>NOTE: The returned object implementing the <tt>CredentialsMatcher</tt> interface need to have a
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
