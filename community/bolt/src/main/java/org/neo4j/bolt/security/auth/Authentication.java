/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.security.auth;

import java.util.Map;

/**
 * Authenticate a given token.
 * <p>
 * The provided token must contain the following items:
 * <ul>
 *  <li><code>scheme</code>: a string defining the authentication scheme.</li>
 *  <li><code>principal</code>: The security principal, the format of the value depends on the authentication
 *  scheme.</li>
 *  <li><code>credentials</code>: The credentials corresponding to the <code>principal</code>, the format of the
 *      value depends on the authentication scheme.</li>
 * </ul>
 * <p>
 *
 * For updating the credentials the new credentials is supplied with the key <code>new_credentials</code>.
 */
public interface Authentication
{
    /**
     * Authenticate the provided token.
     * @param authToken The token to be authenticated.
     * @throws AuthenticationException If authentication failed.
     */
    AuthenticationResult authenticate( Map<String,Object> authToken ) throws AuthenticationException;
}
