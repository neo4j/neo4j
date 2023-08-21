/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.security;

import java.util.Map;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.exceptions.Status;

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
 */
public interface Authentication {
    /**
     * Authenticate the provided token.
     *
     * @param authToken      The token to be authenticated.
     * @param connectionInfo Information about the client connection.
     * @throws AuthenticationException If authentication failed.
     */
    AuthenticationResult authenticate(Map<String, Object> authToken, ClientConnectionInfo connectionInfo)
            throws AuthenticationException;

    default LoginContext impersonate(LoginContext context, String userToImpersonate) throws AuthenticationException {
        throw new AuthenticationException(Status.Security.AuthProviderFailed, "Unsupported operation");
    }
}
