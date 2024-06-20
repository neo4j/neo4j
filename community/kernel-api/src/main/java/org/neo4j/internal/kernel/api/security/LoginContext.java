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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;

import java.util.Objects;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.token.api.TokenConstants;

/**
 * The LoginContext hold the executing authenticated user (subject).
 * By calling {@link #authorize(IdLookup, String, AbstractSecurityLog)} the user is also authorized, and a full SecurityContext is returned,
 * which can be used to assert user permissions during query execution.
 */
public abstract class LoginContext {
    protected final AuthSubject subject;
    private final ClientConnectionInfo connectionInfo;

    public LoginContext(AuthSubject subject, ClientConnectionInfo connectionInfo) {
        this.subject = subject;
        this.connectionInfo = connectionInfo;
    }

    /**
     * Get the authenticated user.
     */
    public AuthSubject subject() {
        return subject;
    }

    public ClientConnectionInfo connectionInfo() {
        return connectionInfo;
    }

    public boolean impersonating() {
        return !Objects.equals(subject.executingUser(), subject.authenticatedUser());
    }

    public boolean nativelyAuthenticated() {
        return subject.nativelyAuthenticated();
    }

    /**
     * Authorize the user and return a SecurityContext.
     *
     * @param idLookup token lookup, used to compile fine grained security verification
     * @param dbName the name of the database the user should be authorized against
     * @param securityLog where to log security related messages
     * @return the security context
     */
    public abstract SecurityContext authorize(IdLookup idLookup, String dbName, AbstractSecurityLog securityLog);

    /**
     * Get a login context with full privileges.
     *
     * @param connectionInfo information about the clients connection.
     */
    public static LoginContext fullAccess(ClientConnectionInfo connectionInfo) {
        return new LoginContext(AuthSubject.AUTH_DISABLED, connectionInfo) {
            @Override
            public SecurityContext authorize(IdLookup idLookup, String dbName, AbstractSecurityLog securityLog) {
                return SecurityContext.authDisabled(AccessMode.Static.FULL, connectionInfo(), dbName);
            }
        };
    }

    /**
     * A login context with full privileges, should only be used for transactions without external connection.
     */
    public static final LoginContext AUTH_DISABLED = fullAccess(EMBEDDED_CONNECTION);

    public interface IdLookup {
        int[] NO_SUCH_PROCEDURE = EMPTY_INT_ARRAY;

        int getPropertyKeyId(String name);

        int getLabelId(String name);

        int getRelTypeId(String name);

        int[] getProcedureIds(String procedureGlobbing);

        int[] getAdminProcedureIds();

        int[] getFunctionIds(String functionGlobbing);

        int[] getAggregatingFunctionIds(String functionGlobbing);

        boolean isCachableLookup();

        boolean isStale();

        IdLookup EMPTY = new IdLookup() {
            @Override
            public int getPropertyKeyId(String name) {
                return TokenConstants.NO_TOKEN;
            }

            @Override
            public int getLabelId(String name) {
                return TokenConstants.NO_TOKEN;
            }

            @Override
            public int getRelTypeId(String name) {
                return TokenConstants.NO_TOKEN;
            }

            @Override
            public int[] getProcedureIds(String procedureGlobbing) {
                return NO_SUCH_PROCEDURE;
            }

            @Override
            public int[] getAdminProcedureIds() {
                return NO_SUCH_PROCEDURE;
            }

            @Override
            public int[] getFunctionIds(String functionGlobbing) {
                return NO_SUCH_PROCEDURE;
            }

            @Override
            public int[] getAggregatingFunctionIds(String functionGlobbing) {
                return NO_SUCH_PROCEDURE;
            }

            @Override
            public boolean isCachableLookup() {
                return false;
            }

            @Override
            public boolean isStale() {
                return true;
            }
        };
    }
}
