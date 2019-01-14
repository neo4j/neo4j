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

import java.util.Collection;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthorizationExpiredException;

/**
 * An authorization provider plugin for the Neo4j enterprise security module.
 *
 * <p>If the configuration setting {@code dbms.security.plugin.authorization_enabled} is set to {@code true},
 * all objects that implements this interface that exists in the class path at Neo4j startup, will be
 * loaded as services.
 *
 * <p>NOTE: If the same object also implements {@link AuthenticationPlugin}, it will not be loaded twice.
 *
 * @see AuthPlugin
 * @see AuthorizationExpiredException
 */
public interface AuthorizationPlugin extends AuthProviderLifecycle
{
    /**
     * An object containing a principal and its corresponding authentication provider.
     */
    final class PrincipalAndProvider
    {
        private final Object principal;
        private final String provider;

        public PrincipalAndProvider( Object principal, String provider )
        {
            this.principal = principal;
            this.provider = provider;
        }

        public Object principal()
        {
            return principal;
        }

        public String provider()
        {
            return provider;
        }
    }

    /**
     * The name of this authorization provider.
     *
     * <p>This name, prepended with the prefix "plugin-", can be used by a client to direct an auth token directly
     * to this authorization provider.
     *
     * @return the name of this authorization provider
     */
    String name();

    /**
     * Should perform authorization of the given collection of principals and their corresponding authentication
     * providers, and return an {@link AuthorizationInfo} result that contains a collection of roles
     * that are assigned to the given principals.
     *
     * @param principals a collection of principals and their corresponding authentication providers
     *
     * @return an {@link AuthorizationInfo} result that contains the roles that are assigned to the given principals
     */
    AuthorizationInfo authorize( Collection<PrincipalAndProvider> principals );

    class Adapter extends AuthProviderLifecycle.Adapter implements AuthorizationPlugin
    {
        @Override
        public String name()
        {
            return getClass().getName();
        }

        @Override
        public AuthorizationInfo authorize( Collection<PrincipalAndProvider> principals )
        {
            return null;
        }
    }
}
