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

import java.util.Collection;

import org.neo4j.server.security.enterprise.auth.plugin.api.RealmOperations;

/**
 * An authorization plugin realm for the Neo4j enterprise security module.
 *
 * <p>If the configuration setting <tt>dbms.security.plugin.authorization_enabled</tt> is set to <tt>true</tt>,
 * all objects that implements this interface that exists in the class path at Neo4j startup, will be
 * loaded as services.
 *
 * <p>NOTE: If the same object also implements <tt>AuthenticationPlugin</tt>, it will not be loaded twice.
 *
 * @see AuthenticationPlugin
 * @see AuthPlugin
 */
public interface AuthorizationPlugin extends RealmLifecycle
{
    /**
     * An object containing a principal and its corresponding realm.
     */
    final class PrincipalAndRealm
    {
        private final Object principal;
        private final String realm;

        public PrincipalAndRealm( Object principal, String realm )
        {
            this.principal = principal;
            this.realm = realm;
        }

        public Object principal()
        {
            return principal;
        }

        public String realm()
        {
            return realm;
        }
    };

    /**
     * The name of this realm.
     *
     * <p>This name, prepended with the prefix "plugin-", can be used by a client to direct an auth token directly
     * to this realm.
     *
     * @return the name of this realm
     */
    String name();

    /**
     * Should perform authorization of the given collection of principals and their corresponding realms (that
     * authenticated them), and return an <tt>AuthorizationInfo</tt> result that contains a collection of roles
     * that are assigned to the given principals.
     *
     * @param principals a collection of principals and their corresponding realms (that authenticated them)
     *
     * @return an <tt>AuthorizationInfo</tt> result that contains the roles that are assigned to the given principals.
     */
    AuthorizationInfo authorize( Collection<PrincipalAndRealm> principals );

    class Adapter extends RealmLifecycle.Adapter implements AuthorizationPlugin
    {
        @Override
        public String name()
        {
            return getClass().getName();
        }

        @Override
        public AuthorizationInfo authorize( Collection<PrincipalAndRealm> principals )
        {
            return null;
        }
    }
}
