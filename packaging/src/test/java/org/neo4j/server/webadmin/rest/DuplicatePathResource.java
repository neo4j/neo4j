package org.neo4j.server.webadmin.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.neo4j.server.database.Database;

@Path( "testing/" )
public class DuplicatePathResource
{

    public DuplicatePathResource( @Context Database database, @Context UriInfo uriInfo )
    {
    }


    @GET
    @Produces( MediaType.APPLICATION_JSON )
    public Response getOne(  )
    {
        return Response.ok( "getOne" ).build();
    }
    @GET
    @Produces( MediaType.APPLICATION_JSON )
    public Response getTwo(  )
    {
        return Response.ok( "getTwo" ).build();
    }

}
