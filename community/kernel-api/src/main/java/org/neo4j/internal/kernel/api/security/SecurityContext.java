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
package org.neo4j.internal.kernel.api.security;

import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;

import java.util.Collections;
import java.util.Set;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.PrivilegeDatabaseReference;
import org.neo4j.messages.MessageUtil;

/**
 * Controls the capabilities of a KernelTransaction, including the authenticated user and authorization data.
 *
 * Must extend LoginContext to handle queries and procedures creating internal transactions, and the parallel cypher prototype.
 */
public class SecurityContext extends LoginContext {
    protected final AccessMode mode;
    private final String database;

    public SecurityContext(AuthSubject subject, AccessMode mode, ClientConnectionInfo connectionInfo, String database) {
        super(subject, connectionInfo);
        this.mode = mode;
        this.database = database;
    }

    /**
     * Get the authorization data of the user. This is immutable.
     */
    public AccessMode mode() {
        return mode;
    }

    public DatabaseAccessMode databaseAccessMode() {
        return DatabaseAccessMode.FULL;
    }

    public String database() {
        return database;
    }

    /**
     * Check whether the user is allowed to execute procedure annotated with @Admin.
     * @return
     */
    public PermissionState allowExecuteAdminProcedure(int procedureId) {
        return PermissionState.EXPLICIT_GRANT;
    }

    /**
     * Check whether the user has a specific level of admin rights.
     * @return
     */
    public PermissionState allowsAdminAction(AdminActionOnResource action) {
        return PermissionState.EXPLICIT_GRANT;
    }

    public Set<String> roles() {
        return Collections.emptySet();
    }

    @Override
    public SecurityContext authorize(
            IdLookup idLookup, PrivilegeDatabaseReference dbReference, AbstractSecurityLog securityLog) {
        return this;
    }

    /**
     * Create a copy of this SecurityContext with the provided mode.
     */
    public SecurityContext withMode(AccessMode mode) {
        return new SecurityContext(subject, mode, connectionInfo(), database());
    }

    /**
     * Create a copy of this SecurityContext with the provided admin access mode.
     */
    public SecurityContext withMode(AdminAccessMode adminAccessMode) {
        return new SecurityContext(subject, mode, connectionInfo(), database());
    }

    public void assertCredentialsNotExpired(SecurityAuthorizationHandler handler) {
        if (AuthenticationResult.PASSWORD_CHANGE_REQUIRED.equals(subject().getAuthenticationResult())) {
            throw handler.logAndGetAuthorizationException(
                    this,
                    SecurityAuthorizationHandler.generateCredentialsExpiredMessage(PERMISSION_DENIED),
                    Status.Security.CredentialsExpired);
        }
    }

    public String description() {
        return MessageUtil.withUser(subject().executingUser(), mode().name());
    }

    protected String defaultString(String name) {
        return String.format("%s{ username=%s, accessMode=%s }", name, subject().executingUser(), mode());
    }

    /** Allows all operations. */
    public static final SecurityContext AUTH_DISABLED = authDisabled(AccessMode.Static.FULL, EMBEDDED_CONNECTION, null);

    public static SecurityContext authDisabled(AccessMode mode, ClientConnectionInfo connectionInfo, String database) {
        return new SecurityContext(AuthSubject.AUTH_DISABLED, mode, connectionInfo, database) {
            @Override
            public SecurityContext withMode(AccessMode mode) {
                return authDisabled(mode, connectionInfo(), database());
            }

            @Override
            public String description() {
                return "AUTH_DISABLED with " + mode().name();
            }

            @Override
            public String toString() {
                return defaultString("auth-disabled");
            }
        };
    }
}
