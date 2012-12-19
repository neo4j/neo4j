/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.webadmin.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.webadmin.rest.console.ConsoleService;
import org.neo4j.server.webadmin.console.ConsoleSessionFactory;
import org.neo4j.server.webadmin.rest.representations.ServerRootRepresentation;

@Path( "/" )
public class RootService
{
    @GET
    public Response getServiceDefinition( @Context UriInfo uriInfo, @Context OutputFormat output )
    {
        ServerRootRepresentation representation = new ServerRootRepresentation( uriInfo.getBaseUri(), services() );

        return output.ok( representation );
    }

    private AdvertisableService[] services()
    {
        AdvertisableService console = new ConsoleService( (ConsoleSessionFactory) null, null, null );
        AdvertisableService jmx = new JmxService( null, null );
        MonitorService monitor = new MonitorService( null, null );

        return new AdvertisableService[] { console, jmx, monitor };
    }

}
