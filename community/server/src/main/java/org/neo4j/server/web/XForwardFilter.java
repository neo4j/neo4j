/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.web;

import java.net.URI;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

import static org.neo4j.server.web.XForwardUtil.X_FORWARD_HOST_HEADER_KEY;
import static org.neo4j.server.web.XForwardUtil.X_FORWARD_PROTO_HEADER_KEY;

/**
 * Changes the value of the base and request URIs to match the provided
 * X-Forwarded-Host and X-Forwarded-Proto header values.
 * <p>
 * In doing so, it means Neo4j server can use those URIs as if they were the
 * actual request URIs.
 */
public class XForwardFilter implements ContainerRequestFilter
{
    @Override
    public ContainerRequest filter( ContainerRequest containerRequest )
    {
        String xForwardedHost = containerRequest.getHeaderValue( X_FORWARD_HOST_HEADER_KEY );
        String xForwardedProto = containerRequest.getHeaderValue( X_FORWARD_PROTO_HEADER_KEY );

        URI externalBaseUri = XForwardUtil.externalUri( containerRequest.getBaseUri(), xForwardedHost, xForwardedProto );
        URI externalRequestUri = XForwardUtil.externalUri( containerRequest.getRequestUri(), xForwardedHost, xForwardedProto );

        containerRequest.setUris( externalBaseUri, externalRequestUri );
        return containerRequest;
    }
}
