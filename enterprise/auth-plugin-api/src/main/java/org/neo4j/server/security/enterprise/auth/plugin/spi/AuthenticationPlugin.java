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
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthenticationException;

/**
 * An authentication provider plugin for the Neo4j enterprise security module.
 *
 * <p>If the configuration setting <tt>dbms.security.plugin.authentication_enabled</tt> is set to <tt>true</tt>,
 * all objects that implements this interface that exists in the class path at Neo4j startup, will be
 * loaded as services.
 *
 * @see AuthPlugin
 * @see AuthorizationPlugin
 */
public interface AuthenticationPlugin extends AuthProviderLifecycle
{
    /**
     * The name of this authentication provider.
     *
     * <p>This name, prepended with the prefix "plugin-", can be used by a client to direct an auth token directly
     * to this authentication provider.
     *
     * @return the name of this authentication provider
     */
    String name();

    /**
     * Should perform authentication of the identity in the given auth token and return an
     * <tt>AuthenticationInfo</tt> result if successful.
     * If authentication failed, either <tt>null</tt> should be returned,
     * or an <tt>AuthenticationException</tt> should be thrown.
     *
     * <p>If authentication caching is enabled, either a <tt>CacheableAuthenticationInfo</tt> or a
     * <tt>CustomCacheableAuthenticationInfo</tt> should be returned.
     *
     * @return an <tt>AuthenticationInfo</tt> object if authentication was successful, otherwise <tt>null</tt>
     *
     * @see org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken
     * @see AuthenticationInfo
     * @see CacheableAuthenticationInfo
     * @see CustomCacheableAuthenticationInfo
     * @see AuthProviderOperations#setAuthenticationCachingEnabled(boolean)
     */
    AuthenticationInfo authenticate( AuthToken authToken ) throws AuthenticationException;

    abstract class Adapter extends AuthProviderLifecycle.Adapter implements AuthenticationPlugin
    {
        @Override
        public String name()
        {
            return getClass().getName();
        }
    }

    abstract class CachingEnabledAdapter extends AuthProviderLifecycle.Adapter implements AuthenticationPlugin
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
