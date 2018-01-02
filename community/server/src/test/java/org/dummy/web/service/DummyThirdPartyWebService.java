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
package org.dummy.web.service;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

@Path( "/" )
public class DummyThirdPartyWebService
{

    public static final String DUMMY_WEB_SERVICE_MOUNT_POINT = "/dummy";

    @GET
    @Produces( MediaType.TEXT_PLAIN )
    public Response sayHello()
    {
        return Response.ok()
                .entity( "hello" )
                .build();
    }


    @GET
    @Path("/{something}/{somethingElse}")
    @Produces( MediaType.TEXT_PLAIN )
    public Response forSecurityTesting() {
        return Response.ok().entity("you've reached a dummy service").build();
    }

    @GET
    @Path( "inject-test" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response countNodes( @Context GraphDatabaseService db )
    {
        try (Transaction transaction = db.beginTx())
        {
            return Response.ok()
                    .entity( String.valueOf( countNodesIn( db ) ) )
                    .build();
        }
    }

    @GET
    @Path( "needs-auth-header" )
    @Produces( MediaType.APPLICATION_JSON )
    public Response authHeader( @Context HttpHeaders headers )
    {
        StringBuilder theEntity = new StringBuilder( "{" );
        Iterator<Map.Entry<String, List<String>>> headerIt = headers.getRequestHeaders().entrySet().iterator();
        while ( headerIt.hasNext() )
        {
            Map.Entry<String, List<String>> header = headerIt.next();
            if (header.getValue().size() != 1)
            {
                throw new IllegalArgumentException( "Mutlivalued header: "
                                                    + header.getKey() );
            }
            theEntity.append( "\"" ).append( header.getKey() ).append( "\":\"" )
                     .append( header.getValue().get( 0 ) ).append( "\"" );
            if ( headerIt.hasNext() )
            {
                theEntity.append( ", " );
            }
        }
        theEntity.append( "}" );
        return Response.ok().entity( theEntity.toString() ).build();
    }

    private int countNodesIn( GraphDatabaseService db )
    {
        int count = 0;
        for ( @SuppressWarnings("unused") Node node : GlobalGraphOperations.at(db).getAllNodes() )
        {
            count++;
        }
        return count;
    }
}
