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

package org.neo4j.server.webadmin.rest;

import static org.neo4j.server.webadmin.rest.WebUtils.addHeaders;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.rest.domain.JsonRenderers;
import org.neo4j.server.webadmin.domain.ServerRootRepresentation;
import org.neo4j.server.webadmin.domain.ServerRootRepresentation.Mode;

/**
 * Serves info to clients about what is available on this management server.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
@Path( RootService.ROOT_PATH )
public class RootService
{

    public static final String ROOT_PATH = "/";

    @GET
    @Produces( MediaType.APPLICATION_JSON )
    public Response getServiceDefinition( @Context UriInfo uriInfo )
    {
        Mode serverMode = Mode.EMBEDDED;

        String entity = JsonRenderers.DEFAULT.render( new ServerRootRepresentation(
                uriInfo.getBaseUri(), serverMode ) );

        return addHeaders(
                Response.ok( entity, JsonRenderers.DEFAULT.getMediaType() ) ).build();
    }

}
