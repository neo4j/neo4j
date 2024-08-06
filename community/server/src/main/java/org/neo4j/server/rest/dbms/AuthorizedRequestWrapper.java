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
package org.neo4j.server.rest.dbms;

import java.security.Principal;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.database.PrivilegeDatabaseReference;
import org.neo4j.server.rest.web.HttpConnectionInfoFactory;

public class AuthorizedRequestWrapper extends HttpServletRequestWrapper {
    public static LoginContext getLoginContextFromHttpServletRequest(HttpServletRequest request) {
        Principal principal = request.getUserPrincipal();
        ClientConnectionInfo connectionInfo = HttpConnectionInfoFactory.create(request);
        return getLoginContextFromUserPrincipal(principal, connectionInfo);
    }

    public static LoginContext getLoginContextFromUserPrincipal(
            Principal principal, ClientConnectionInfo connectionInfo) {
        if (principal instanceof DelegatingPrincipal) {
            return ((DelegatingPrincipal) principal).getLoginContext();
        }
        // If whitelisted uris can start transactions we cannot throw exception here
        // throw new IllegalArgumentException( "Tried to get access mode on illegal user principal" );
        return new LoginContext(AuthSubject.ANONYMOUS, connectionInfo) {
            @Override
            public SecurityContext authorize(
                    IdLookup idLookup, PrivilegeDatabaseReference dbReference, AbstractSecurityLog securityLog) {
                return new SecurityContext(subject(), AccessMode.Static.ACCESS, connectionInfo(), dbReference.name());
            }
        };
    }

    private final String authType;
    private final DelegatingPrincipal principal;

    public AuthorizedRequestWrapper(
            final String authType, final String username, final HttpServletRequest request, LoginContext loginContext) {
        super(request);
        this.authType = authType;
        this.principal = new DelegatingPrincipal(username, loginContext);
    }

    @Override
    public String getAuthType() {
        return authType;
    }

    @Override
    public Principal getUserPrincipal() {
        return principal;
    }

    @Override
    public boolean isUserInRole(String role) {
        return true;
    }

    @Override
    public String toString() {
        return "AuthorizedRequestWrapper{" + "authType='" + authType + '\'' + ", principal=" + principal + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AuthorizedRequestWrapper that = (AuthorizedRequestWrapper) o;
        if (!authType.equals(that.authType)) {
            return false;
        }
        return principal.equals(that.principal);
    }

    @Override
    public int hashCode() {
        int result = authType.hashCode();
        result = 31 * result + principal.hashCode();
        return result;
    }
}
