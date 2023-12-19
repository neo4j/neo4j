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
import org.neo4j.server.security.enterprise.auth.plugin.api.AuthenticationException;

/**
 * An authentication provider plugin for the Neo4j enterprise security module.
 *
 * <p>If the configuration setting {@code dbms.security.plugin.authentication_enabled} is set to {@code true},
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
     * {@link AuthenticationInfo} result if successful.
     * If authentication failed, either {@code null} should be returned,
     * or an {@link AuthenticationException} should be thrown.
     * <p>
     * If authentication caching is enabled, either a {@link CacheableAuthenticationInfo} or a
     * {@link CustomCacheableAuthenticationInfo} should be returned.
     *
     * @return an {@link AuthenticationInfo} object if authentication was successful, otherwise {@code null}
     * @throws AuthenticationException if authentication failed
     *
     * @see org.neo4j.server.security.enterprise.auth.plugin.api.AuthToken
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
        public void initialize( AuthProviderOperations authProviderOperations )
        {
            authProviderOperations.setAuthenticationCachingEnabled( true );
        }
    }
}
