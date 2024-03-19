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

import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.server.rest.web.HttpConnectionInfoFactory;
import org.neo4j.server.web.JettyHttpConnection;

public class AuthorizationDisabledFilter extends AuthorizationFilter {
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        validateRequestType(servletRequest);
        validateResponseType(servletResponse);

        final HttpServletRequest request = (HttpServletRequest) servletRequest;
        final HttpServletResponse response = (HttpServletResponse) servletResponse;

        try {
            ClientConnectionInfo connectionInfo = HttpConnectionInfoFactory.create(request);
            LoginContext loginContext = getAuthDisabledLoginContext(connectionInfo);
            String userAgent = request.getHeader(HttpHeaders.USER_AGENT);

            JettyHttpConnection.updateUserForCurrentConnection(
                    loginContext.subject().executingUser(), userAgent);

            filterChain.doFilter(new AuthorizedRequestWrapper(null, "neo4j", request, loginContext), servletResponse);
        } catch (AuthorizationViolationException e) {
            unauthorizedAccess(e.getMessage()).accept(response);
        }
    }

    protected LoginContext getAuthDisabledLoginContext(ClientConnectionInfo connectionInfo) {
        return LoginContext.fullAccess(connectionInfo);
    }
}
