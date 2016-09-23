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
package org.neo4j.server.security.enterprise.auth.plugin.api;

import java.util.Map;

/**
 * The authentication token provided by the client, which is used to authenticate the subject's identity.
 *
 * <p>A common scenario is to have principal be a username and credentials be a password.
 *
 * @see org.neo4j.server.security.enterprise.auth.plugin.spi.AuthenticationPlugin#authenticate(AuthToken)
 * @see org.neo4j.server.security.enterprise.auth.plugin.spi.AuthPlugin#authenticateAndAuthorize(AuthToken)
 */
public interface AuthToken
{
    /**
     * Returns the identity to authenticate.
     *
     * <p>Most commonly this is a username.
     *
     * @return the identity to authenticate.
     */
    String principal();

    /**
     * Returns the credentials that verifies the identity.
     *
     * <p>Most commonly this is a password.
     *
     * <p>The reason this is a character array and not a <tt>String</tt>, is so that sensitive information
     * can be cleared from memory after useage without having to wait for the garbage collector to act.
     *
     * @return the credentials that verifies the identity.
     */
    char[] credentials();

    /**
     * Returns an optional custom parameter map if provided by the client.
     *
     * <p>This can be used as a vehicle to send arbitrary auth data from a client application
     * to a server-side auth plugin. Neo4j will act as a pure transport and will not touch the contents of this map.
     *
     * @return a custom parameter map if provided by the client, otherwise <tt>null</tt>
     */
    Map<String,Object> parameters();
}
