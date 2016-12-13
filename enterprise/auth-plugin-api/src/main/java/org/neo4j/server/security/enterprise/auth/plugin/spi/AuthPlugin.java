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
package org.neo4j.server.security.enterprise.auth.plugin.spi;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthProviderOperations;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthenticationException;

/**
 * A simplified combined authentication and authorization provider plugin for the Neo4j enterprise security module.
 *
 * <p>If either the configuration setting <tt>dbms.security.plugin.authentication_enabled</tt> or
 * <tt>dbms.security.plugin.authorization_enabled</tt> is set to <tt>true</tt>,
 * all objects that implements this interface that exists in the class path at Neo4j startup, will be
 * loaded as services.
 *
 * @see AuthenticationPlugin
 * @see AuthorizationPlugin
 */
public interface AuthPlugin extends AuthProviderLifecycle
{
    /**
     * The name of this auth provider.
     *
     * <p>This name, prepended with the prefix "plugin-", can be used by a client to direct an auth token directly
     * to this auth provider.
     *
     * @return the name of this auth provider
     */
    String name();

    /**
     * Should perform both authentication and authorization of the identity in the given auth token and return an
     * <tt>AuthInfo</tt> result if successful. The <tt>AuthInfo</tt> result can also contain a collection of roles
     * that are assigned to the given identity, which constitutes the authorization part.
     *
     * If authentication failed, either <tt>null</tt> should be returned,
     * or an <tt>AuthenticationException</tt> should be thrown.
     *
     * <p>If authentication caching is enabled, then a <tt>CacheableAuthInfo</tt> should be returned.
     *
     * @return an <tt>AuthInfo</tt> object if authentication was successful, otherwise <tt>null</tt>
     *
     * @see org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken
     * @see AuthenticationInfo
     * @see CacheableAuthenticationInfo
     * @see CustomCacheableAuthenticationInfo
     * @see AuthProviderOperations#setAuthenticationCachingEnabled(boolean)
     */
    AuthInfo authenticateAndAuthorize( AuthToken authToken ) throws AuthenticationException;

    abstract class Adapter extends AuthProviderLifecycle.Adapter implements AuthPlugin
    {
        @Override
        public String name()
        {
            return getClass().getName();
        }
    }

    abstract class CachingEnabledAdapter extends AuthProviderLifecycle.Adapter implements AuthPlugin
    {
        @Override
        public String name()
        {
            return getClass().getName();
        }

        @Override
        public void initialize( AuthProviderOperations authProviderOperations ) throws Throwable
        {
            authProviderOperations.setAuthenticationCachingEnabled( true );
        }
    }
}
