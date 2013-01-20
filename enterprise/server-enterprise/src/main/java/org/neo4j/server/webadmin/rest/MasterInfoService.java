/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.OutputFormat;

@Path( MasterInfoService.BASE_PATH )
public class MasterInfoService implements AdvertisableService
{
    public static final String BASE_PATH = "/server/ha";
    public static final String ISMASTER_PATH = "/isMaster";
    public static final String GETMASTER_PATH = "/getMaster";

    private final OutputFormat output;
    private final HighlyAvailableGraphDatabase haDb;

    public MasterInfoService( @Context OutputFormat output, @Context GraphDatabaseService db )
    {
        this.output = output;
        if ( db instanceof HighlyAvailableGraphDatabase )
        {
            this.haDb = (HighlyAvailableGraphDatabase) db;
        }
        else
        {
            this.haDb = null;
        }
    }

    @GET
    public Response discover() throws BadInputException
    {
        if ( haDb == null )
        {
            return Response.status( Response.Status.FORBIDDEN ).build();
        }
        String isMasterUri = "isMaster";
        String getMasterUri = "getMaster";

        HaDiscoveryRepresentation dr = new HaDiscoveryRepresentation( isMasterUri, getMasterUri );
        return output.ok( dr );
    }

    @GET
    @Path( ISMASTER_PATH )
    public Response isMaster() throws BadInputException
    {
        if ( haDb == null )
        {
            return Response.status( Response.Status.FORBIDDEN ).build();
        }
        if ( haDb.isMaster() )
        {
            return Response.status( Response.Status.OK ).entity( Boolean.toString( true ).getBytes()).build();
        }
        else
        {
            return Response.status( Response.Status.SEE_OTHER ).entity( Boolean.toString( false ).getBytes() ).header(
                    HttpHeaders.LOCATION, getMasterUriAsString() ).build();
        }
    }

    @GET
    @Path( GETMASTER_PATH )
    public Response getMaster() throws BadInputException
    {
        if ( haDb == null )
        {
            return Response.status( Response.Status.FORBIDDEN ).build();
        }
        String masterURI = getMasterUriAsString();
        return Response.status( Response.Status.SEE_OTHER ).entity( Boolean.toString( false ).getBytes() ).header(
                HttpHeaders.LOCATION, masterURI ).build();
    }

    private String getMasterUriAsString()
    {
        String masterURI = "not found";
        for (ClusterMember member : haDb.getDependencyResolver().resolveDependency( ClusterMembers.class ).getMembers() )
        {
            if ( HighAvailabilityModeSwitcher.MASTER.equals( member.getHARole() ) )
            {
                masterURI = member.getHAUri().getHost() + ":" + member.getHAUri().getPort();
            }
        }
        return masterURI;
    }

    @Override
    public String getName()
    {
        return "ha";
    }

    @Override
    public String getServerPath()
    {
        return BASE_PATH;
    }
}
