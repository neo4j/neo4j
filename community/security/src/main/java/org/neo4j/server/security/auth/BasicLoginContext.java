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
package org.neo4j.server.security.auth;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.FAILURE;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.PASSWORD_CHANGE_REQUIRED;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.TOO_MANY_ATTEMPTS;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.PrivilegeDatabaseReference;
import org.neo4j.kernel.impl.security.User;

public class BasicLoginContext extends LoginContext {
    private final AccessMode accessMode;

    public BasicLoginContext(
            User user, AuthenticationResult authenticationResult, ClientConnectionInfo connectionInfo) {
        super(new BasicAuthSubject(user, authenticationResult), connectionInfo);

        switch (authenticationResult) {
            case SUCCESS:
                accessMode = AccessMode.Static.FULL;
                break;
            case PASSWORD_CHANGE_REQUIRED:
                accessMode = AccessMode.Static.CREDENTIALS_EXPIRED;
                break;
            default:
                accessMode = AccessMode.Static.ACCESS;
        }
    }

    private static class BasicAuthSubject implements AuthSubject {
        private final User user;
        private final AuthenticationResult authenticationResult;

        BasicAuthSubject(User user, AuthenticationResult authenticationResult) {
            this.user = user;
            this.authenticationResult = authenticationResult;
        }

        @Override
        public AuthenticationResult getAuthenticationResult() {
            return authenticationResult;
        }

        @Override
        public String executingUser() {
            if (user != null) {
                return user.name();
            }
            return ""; // could be the case if user not exists
        }

        @Override
        public boolean hasUsername(String username) {
            return executingUser().equals(username);
        }
    }

    @Override
    public SecurityContext authorize(
            IdLookup idLookup, PrivilegeDatabaseReference dbReference, AbstractSecurityLog securityLog) {
        String dbName = dbReference.name();
        SecurityContext securityContext = new SecurityContext(subject(), accessMode, connectionInfo(), dbName);
        if (subject().getAuthenticationResult().equals(FAILURE)
                || subject().getAuthenticationResult().equals(TOO_MANY_ATTEMPTS)) {
            securityLog.error(securityContext, String.format("Authentication failed for database '%s'.", dbName));
            throw new AuthorizationViolationException(
                    AuthorizationViolationException.PERMISSION_DENIED, Status.Security.Unauthorized);
        } else if (!dbName.equals(SYSTEM_DATABASE_NAME)
                && subject().getAuthenticationResult().equals(PASSWORD_CHANGE_REQUIRED)) {
            String message = SecurityAuthorizationHandler.generateCredentialsExpiredMessage(
                    String.format("ACCESS on database '%s' is not allowed.", dbName));
            securityLog.error(securityContext, message);
            throw new AuthorizationViolationException(message, Status.Security.CredentialsExpired);
        }
        return securityContext;
    }
}
