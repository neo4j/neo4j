/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.rest.web.security;

import java.security.Principal;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import org.neo4j.server.NeoServer;
import org.neo4j.server.database.DatabaseBlockedException;

import com.sun.jersey.api.container.MappableContainerException;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

/**
 * Security filter based on sun's example implementation.
 * 
 * This will deny all requests that do not authenticate themselves using HTTP
 * basic authentication.
 */
public class SecurityFilter implements ContainerRequestFilter {

    public static final String REST_USERNAME = "rest_username";
    public static final String REST_PASSWORD = "rest_password";
    public static final String REST_ENABLE_AUTHENTICATION = "rest_enable_authentication";

    @Context
    protected UriInfo uriInfo;

    protected static final String REALM = "Neo4j REST server";

    public class Authorizer implements SecurityContext {

        private User user;
        private Principal principal;

        public Authorizer(final User user) {
            this.user = user;
            this.principal = new Principal() {

                public String getName() {
                    return user.username;
                }
            };
        }

        public Principal getUserPrincipal() {
            return this.principal;
        }

        public boolean isUserInRole(String role) {
            return (role.equals(user.role));
        }

        public boolean isSecure() {
            return "https".equals(uriInfo.getRequestUri().getScheme());
        }

        public String getAuthenticationScheme() {
            return SecurityContext.BASIC_AUTH;
        }
    }

    public class User {

        public String username;
        public String role;

        public User(String username, String role) {
            this.username = username;
            this.role = role;
        }
    }

    //
    // FILTER
    //

    public ContainerRequest filter(ContainerRequest request) {
        try {
            User user = authenticate(request);
            request.setSecurityContext(new Authorizer(user));
        } catch (DatabaseBlockedException e) {

        }
        return request;
    }

    //
    // INTERNALS
    //

    private User authenticate(ContainerRequest request) throws DatabaseBlockedException {

        //
        // 1. Check if authentication is enabled
        //

        String authenticatedEnabledProperty = (String) NeoServer.server().configuration().getProperty(REST_ENABLE_AUTHENTICATION);
        if (authenticatedEnabledProperty == null || authenticatedEnabledProperty.toString().toLowerCase().equals("false")) {
            return new User("unauthenticated-user", "default");
        }

        //
        // 2. Extract user name and password
        //

        String authentication = request.getHeaderValue(ContainerRequest.AUTHORIZATION);
        if (authentication == null) {
            // Request http authentication
            throw new MappableContainerException(new AuthenticationException("Authorization required", REALM));
        }

        if (!authentication.startsWith("Basic ")) {
            // "Only HTTP Basic authentication is supported"
            throw new WebApplicationException(400);
        }
        authentication = authentication.substring("Basic ".length());
        String[] values = new String(Base64.base64Decode(authentication)).split(":");
        if (values.length < 2) {
            // "Invalid syntax for username and password"
            throw new WebApplicationException(400);
        }
        String username = values[0];
        String password = values[1];
        if ((username == null) || (password == null)) {
            // "Missing username or password"
            throw new WebApplicationException(400);
        }

        //
        // 3. Validate the extracted credentials
        //

        User user = null;
        String expectedUsername = (String) (NeoServer.server().configuration().containsKey(REST_USERNAME) ? NeoServer.server().configuration().getProperty(REST_USERNAME) : "");
        String expectedPassword = (String) (NeoServer.server().configuration().containsKey(REST_PASSWORD) ? NeoServer.server().configuration().getProperty(REST_PASSWORD) : "");

        if (username.equals(expectedUsername) && password.equals(expectedPassword)) {
            user = new User(expectedUsername, "default");
        } else {
            // Authentication failed
            throw new MappableContainerException(new AuthenticationException("Invalid username or password", REALM));
        }

        return user;
    }
}
