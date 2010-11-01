package org.neo4j.server.web;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path(HelloWorldWebResource.ROOT_PATH)
public class HelloWorldWebResource {
    public static final String ROOT_PATH = "/greeting";

    @Consumes("text/plain")
    @Produces("text/plain")
    @POST
    public Response sayHello(String name) {
        return Response.ok().entity("hello, " + name).type("text/plain").build();
    }
}
