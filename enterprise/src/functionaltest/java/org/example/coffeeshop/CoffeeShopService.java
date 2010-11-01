package org.example.coffeeshop;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path("/coffeeshop")
public class CoffeeShopService {
    @Path("menu")
    @Produces("text/html")
    @GET
    public Response getMenu() {
        return Response.ok().entity("<p>lattes for a fiver</p><p>cappuccino for two quid</p><p>espresso for a quid</p>").type("text/html").build();
    }
}
