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
package org.dummy.web.service;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

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
        return Response.ok()
                .entity( String.valueOf( countNodesIn( db ) ) )
                .build();
    }

    @GET
    @Path( "needs-auth-header" )
    @Produces( MediaType.APPLICATION_JSON )
    public Response authHeader( @Context HttpHeaders headers )
    {
        StringBuffer theEntity = new StringBuffer( "{" );
        Iterator<Map.Entry<String, List<String>>> headerIt = headers.getRequestHeaders().entrySet().iterator();
        while ( headerIt.hasNext() )
        {
            Map.Entry<String, List<String>> header = headerIt.next();
            if (header.getValue().size() != 1)
            {
                throw new IllegalArgumentException( "Multivalued header: "
                                                    + header.getKey() );
            }
            theEntity.append( "\"" ).append( header.getKey() ).append( "\":\"" ).append(
                    header.getValue().get( 0 ) + "\"" );
            if ( headerIt.hasNext() )
            {
                theEntity.append( ", " );
            }
        }
        theEntity.append( "}" );
        return Response.ok().entity( theEntity.toString() ).build();
    }

    @POST
    @Path( "json-parse" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response readJson( String json ) throws IOException
    {
        JsonNode jsonObject = new ObjectMapper().readTree( json );
        return Response.ok().entity( jsonObject.get("text").getValueAsText() ).build();
    }

    @POST
    @Path( "json-bean" )
    @Consumes( MediaType.APPLICATION_JSON )
    @Produces( MediaType.APPLICATION_JSON )
    public Response readBeanJson( Input input )
    {
        return Response.ok().entity( new Output( input.text + input.numbers.get(1) ) ).build();
    }

    public static class Input
    {
        public String text;
        public List<Long> numbers;
    }

    public static class Output
    {
        public String out;

        public Output(String out) {
            this.out = out;
        }
    }

    private int countNodesIn( GraphDatabaseService db )
    {
        int count = 0;
        Iterator<Node> nodes = db.getAllNodes()
                .iterator();
        while ( nodes.hasNext() )
        {
            nodes.next();
            count++;
        }
        return count;
    }
}
