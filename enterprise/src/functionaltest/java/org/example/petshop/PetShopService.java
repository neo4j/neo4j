package org.example.petshop;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path("/petshop")
public class PetShopService {
    @Path("prices")
    @Produces("text/html")
    @GET
    public Response getStock() {
        return Response.ok().entity("<p>dogs for a tenner</p><p>cats for a fiver</p><p>goldfish for a squid</p>").type("text/html").build();
    }
}
