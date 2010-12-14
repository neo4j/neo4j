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

import org.neo4j.server.rest.domain.renderers.JsonRenderers;
import org.neo4j.server.webadmin.rest.representations.ServerRootRepresentation;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Path( "/" )
public class RootService
{
	@GET
	@Produces( MediaType.APPLICATION_JSON )
	public Response getServiceDefinition( @Context UriInfo uriInfo )
	{

		ServerRootRepresentation rootRepresentation = new ServerRootRepresentation( uriInfo.getBaseUri(),
				new ConsoleService( (SessionFactory)null,null, null ),
				new JmxService(null,null),
				new MonitorService( null ) );

		String entity = JsonRenderers.DEFAULT.render( rootRepresentation );

		return Response.ok( entity ).header( "Content-Type", MediaType.APPLICATION_JSON ).build();
	}
}
