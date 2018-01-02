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
package org.neo4j.server.rest.web;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.repr.RepresentationWriteHandler;
import org.neo4j.tooling.GlobalGraphOperations;

@Path( "/relationship/types" )
public class DatabaseMetadataService
{
    private final Database database;
    private RepresentationWriteHandler representationWriteHandler = RepresentationWriteHandler.DO_NOTHING;

    public DatabaseMetadataService( @Context Database database )
    {
        this.database = database;
    }

    @GET
    @Produces( MediaType.APPLICATION_JSON )
    public Response getRelationshipTypes( @QueryParam( "in_use" ) @DefaultValue( "true" ) boolean inUse )
    {
        try
        {
            GlobalGraphOperations operations = GlobalGraphOperations.at( database.getGraph() );
            Iterable<RelationshipType> relationshipTypes = inUse
                                                           ? operations.getAllRelationshipTypesInUse()
                                                           : operations.getAllRelationshipTypes();
            return Response.ok()
                    .type( MediaType.APPLICATION_JSON )
                    .entity( generateJsonRepresentation( relationshipTypes ) )
                    .build();
        }
        finally
        {
            representationWriteHandler.onRepresentationFinal();
        }
    }

    private String generateJsonRepresentation( Iterable<RelationshipType> relationshipTypes )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "[" );
        for ( RelationshipType rt : relationshipTypes )
        {
            sb.append( "\"" );
            sb.append( rt.name() );
            sb.append( "\"," );
        }
        sb.append( "]" );
        return sb.toString()
                .replaceAll( ",]", "]" );
    }

    public void setRepresentationWriteHandler( RepresentationWriteHandler representationWriteHandler )
    {
        this.representationWriteHandler = representationWriteHandler;
    }
}
