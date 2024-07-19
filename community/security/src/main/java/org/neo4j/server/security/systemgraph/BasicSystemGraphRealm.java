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
package org.neo4j.server.security.systemgraph;

import static org.neo4j.kernel.api.security.AuthToken.invalidToken;

import java.util.Map;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicLoginContext;

/**
 * Shiro realm using a Neo4j graph to store users
 */
public class BasicSystemGraphRealm extends AuthManager {
    private final SecurityGraphHelper securityGraphHelper;
    private final AuthenticationStrategy authenticationStrategy;

    public BasicSystemGraphRealm(
            SecurityGraphHelper securityGraphHelper, AuthenticationStrategy authenticationStrategy) {
        this.securityGraphHelper = securityGraphHelper;
        this.authenticationStrategy = authenticationStrategy;
    }

    @Override
    public LoginContext login(Map<String, Object> authToken, ClientConnectionInfo connectionInfo)
            throws InvalidAuthTokenException {
        try {
            assertValidScheme(authToken);

            String username = AuthToken.safeCast(AuthToken.PRINCIPAL, authToken);
            byte[] password = AuthToken.safeCastCredentials(AuthToken.CREDENTIALS, authToken);

            User user = securityGraphHelper.getUserByName(username);
            if (user == null) {
                return new BasicLoginContext(null, AuthenticationResult.FAILURE, connectionInfo);
            }
            AuthenticationResult result = authenticationStrategy.authenticate(user, password);
            if (result == AuthenticationResult.SUCCESS && user.passwordChangeRequired()) {
                result = AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
            }
            return new BasicLoginContext(user, result, connectionInfo);
        } finally {
            AuthToken.clearCredentials(authToken);
        }
    }

    @Override
    public LoginContext impersonate(LoginContext originalAuth, String userToImpersonate) {
        throw new InvalidArgumentException("Impersonation is not supported in community edition.");
    }

    private static void assertValidScheme(Map<String, Object> token) throws InvalidAuthTokenException {
        String scheme = AuthToken.safeCast(AuthToken.SCHEME_KEY, token);
        if (scheme.equals("none")) {
            throw invalidToken(", scheme 'none' is only allowed when auth is disabled.");
        }
        if (!scheme.equals(AuthToken.BASIC_SCHEME)) {
            throw invalidToken(", scheme '" + scheme + "' is not supported.");
        }
    }
}
