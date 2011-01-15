package org.neo4j.examples.server.unmanaged;

//START SNIPPET: All
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.log4j.Logger;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.webadmin.rest.SessionFactoryImpl;

@Path( "/helloworld" )
public class HelloWorldResource
{

    private final Database database;

    public HelloWorldResource( @Context Database database,
            @Context HttpServletRequest req, @Context OutputFormat output )
    {
        this( new SessionFactoryImpl( req.getSession( true ) ), database,
                output );
    }

    public HelloWorldResource( SessionFactoryImpl sessionFactoryImpl,
            Database database, OutputFormat output )
    {
        this.database = database;
    }

    @GET
    @Produces( MediaType.TEXT_PLAIN )
    @Path( "/{nodeId}" )
    public Response hello( @PathParam( "nodeId" ) long nodeId )
    {
        // Do stuff with the database
        return Response.status( Status.OK ).entity(
                ( "Hello World, nodeId=" + nodeId).getBytes() ).build();
    }
}
// END SNIPPET: All

