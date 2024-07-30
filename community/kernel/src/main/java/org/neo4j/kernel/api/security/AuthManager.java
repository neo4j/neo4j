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
package org.neo4j.kernel.api.security;

import java.util.Map;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlMessageParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * An AuthManager is used to do basic authentication and user management.
 */
public abstract class AuthManager extends LifecycleAdapter {
    public static final String INITIAL_USER_NAME = "neo4j";
    public static final String INITIAL_PASSWORD = "neo4j";

    /**
     * Log in using the provided authentication token
     *
     * NOTE: The authToken will be cleared of any credentials
     *
     * @param authToken The authentication token to login with. Typically contains principals and credentials.
     * @param connectionInfo The connection information of the request.
     * @return An AuthSubject representing the newly logged-in user
     * @throws InvalidAuthTokenException if the authentication token is malformed
     */
    public abstract LoginContext login(Map<String, Object> authToken, ClientConnectionInfo connectionInfo)
            throws InvalidAuthTokenException;

    public abstract LoginContext impersonate(LoginContext originalAuth, String userToImpersonate);

    /**
     * Implementation that does no authentication.
     */
    public static final AuthManager NO_AUTH = new AuthManager() {
        @Override
        public LoginContext login(Map<String, Object> authToken, ClientConnectionInfo connectionInfo) {
            AuthToken.clearCredentials(authToken);
            return LoginContext.fullAccess(connectionInfo);
        }

        @Override
        public LoginContext impersonate(LoginContext originalAuth, String userToImpersonate) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N30)
                    .withClassification(ErrorClassification.CLIENT_ERROR)
                    .withParam(GqlMessageParams.thing, "Impersonation")
                    .withParam(GqlMessageParams.context, "a database with auth disabled")
                    .build();
            throw new InvalidArgumentException(gql, "Impersonation is not supported with auth disabled.");
        }
    };
}
