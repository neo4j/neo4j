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
package org.neo4j.server.rest.management;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.kernel.KernelData;
import org.neo4j.server.NeoServer;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;

@Path(VersionAndEditionService.SERVER_PATH)
public class VersionAndEditionService implements AdvertisableService
{
    private NeoServer neoServer;
    public static final String SERVER_PATH = "server/version";

    public VersionAndEditionService( @Context NeoServer neoServer )
    {
        this.neoServer = neoServer;
    }

    @Override
    public String getName()
    {
        return "version";
    }

    @Override
    public String getServerPath()
    {
        return SERVER_PATH;
    }

    @GET
    @Produces(APPLICATION_JSON)
    public Response getVersionAndEditionData()
    {
        return Response.ok( createJsonFrom( map(
                        "version", neoDatabaseVersion( neoServer ),
                        "edition", neoServerEdition( neoServer ) ) ),
                APPLICATION_JSON )
                .build();
    }

    private String neoDatabaseVersion( NeoServer neoServer )
    {
        return neoServer.getDatabase().getGraph().getDependencyResolver().resolveDependency( KernelData.class )
                .version().getReleaseVersion();
    }

    private String neoServerEdition( NeoServer neoServer )
    {
        String serverClassName = neoServer.getClass().getName().toLowerCase();

        if ( serverClassName.contains( "enterpriseneoserver" ) )
        {
            return "enterprise";
        }
        else if ( serverClassName.contains( "advancedneoserver" ) )
        {
            return "advanced";
        }
        else if ( serverClassName.contains( "communityneoserver" ) )
        {
            return "community";
        }
        else
        {
//            return "unknown";
            throw new IllegalStateException( "The Neo Server running is of unknown type. Valid types are Community, " +
                    "Advanced, and Enterprise." );
        }
    }
}
